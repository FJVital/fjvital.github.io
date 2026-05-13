import os
import io
import uuid
import csv
import stripe
import boto3
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel
from typing import List
from pathlib import Path

import database
import auth
from orchestrator import run_orchestrator

app = FastAPI()

# --- 1. HARDENED CORS CONFIGURATION ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://flashfix.io",
        "https://www.flashfix.io",
        "https://fjvital.github.io",
        "http://localhost:5500",
        "http://127.0.0.1:5500"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CLOUD ENVIRONMENT KEYS ---
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY")
if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

# AWS S3 SETUP
AWS_BUCKET_NAME = os.environ.get("AWS_BUCKET_NAME", "schema-engine-bucket-1")
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name=os.environ.get("AWS_REGION", "us-east-1")
)

UPLOAD_DIR = "temp_uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.get("/")
def health_check():
    return {"status": "Schema-Sync Live"}

# --- AUTHENTICATION ---
class LoginRequest(BaseModel):
    username: str
    password: str = ""

@app.post("/token")
async def login(req: LoginRequest):
    email = req.username.strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email is required")
        
    user = database.get_user(email)
    if not user:
        print(f"User {email} not found. Auto-registering...")
        database.create_user(email)
        
    token = auth.create_access_token(data={"sub": email})
    return {"access_token": token, "token_type": "bearer"}

# --- CSV PROCESSING ---
@app.post("/quote")
async def get_quote(file: UploadFile = File(...), current_user: str = Depends(auth.get_current_user)):
    job_id = str(uuid.uuid4())
    
    input_path = os.path.join(UPLOAD_DIR, f"input_{job_id}.csv")
    output_path = os.path.join(UPLOAD_DIR, f"output_{job_id}.csv")

    try:
        contents = await file.read()
        
        # Shield: Empty File Check
        if not contents:
            raise HTTPException(status_code=400, detail="The uploaded file is empty.")
        
        if file.filename.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(io.BytesIO(contents))
        elif file.filename.lower().endswith('.csv'):
            df = pd.read_csv(io.BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Please upload a .csv or .xlsx file.")

        df.to_csv(input_path, index=False)
        
        # --- TRIGGER AI ORCHESTRATOR ---
        run_orchestrator(input_path, output_path)
        
        if os.path.exists(output_path):
            final_df = pd.read_csv(output_path)
            
            # --- ALIEXPRESS IMAGE FIX ---
            if 'Image Src' in final_df.columns:
                final_df['Image Src'] = final_df['Image Src'].apply(
                    lambda x: 'https:' + x if isinstance(x, str) and x.startswith('//') else x
                )

            # --- SHOPIFY HEADER GUARD ---
            mandatory_headers = ["Option2 Name", "Option2 Value", "Option3 Name", "Option3 Value"]
            for header in mandatory_headers:
                if header not in final_df.columns:
                    final_df[header] = ""
            
            final_df.to_csv(output_path, index=False)
                
            # EXTRACT PREVIEW DATA (Float64 Fix Applied)
            final_df = final_df.astype(object).fillna("")
            row_count = len(final_df)
            total_price = max(500, row_count * 10) 
            headers = final_df.columns.tolist()
            preview_data = final_df.head(5).to_dict(orient="records")

            database.create_job(job_id, current_user, input_path, output_path, total_price, file.filename)

        else:
            raise HTTPException(status_code=500, detail="Orchestrator failed to create output file.")

    except Exception as e:
        print(f"[ERROR] Formatting failed: {str(e)}")
        # Sanitized Security Error
        raise HTTPException(status_code=500, detail="Data formatting failed. Please check your CSV structure or contact support.")

    return {"job_id": job_id, "rows": row_count, "price": total_price, "headers": headers, "preview": preview_data}

# --- PAYMENT PROCESSING ---
@app.get("/create-payment-intent/{job_id}")
async def create_payment_intent(job_id: str, current_user: str = Depends(auth.get_current_user)):
    job = database.get_job(job_id)
    user = database.get_user(current_user)

    if not job or not user:
        raise HTTPException(status_code=404, detail="Job or user not found.")

    price = job["price"] 

    try:
        # STRIPE EMAIL INJECTION FOR AUTOMATIC RECEIPTS
        if user.get("stripe_customer_id"):
            intent = stripe.PaymentIntent.create(
                amount=price,
                currency='usd',
                customer=user["stripe_customer_id"],
                receipt_email=user["username"]
            )
        else:
            intent = stripe.PaymentIntent.create(
                amount=price,
                currency='usd',
                receipt_email=user["username"]
            )
            
        return {"clientSecret": intent.client_secret}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- FILE RETRIEVAL & VERIFICATION ---
@app.post("/verify-payment/{job_id}")
async def verify_payment(job_id: str, request: Request, current_user: str = Depends(auth.get_current_user)):
    job = database.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    database.update_job_status(job_id, "paid")
    return {"status": "success"} 

@app.get("/download/{job_id}")
async def download(job_id: str, token: str = None):
    # Authenticate token if provided
    if token:
        user = auth.get_user_from_token(token)
        if not user:
            raise HTTPException(status_code=401, detail="Unauthorized token.")

    job = database.get_job(job_id)
    
    # Check if job is paid (handles both dict styles based on your DB)
    is_paid = job and (job.get("status") == "paid" or job.get("paid") == True)
    
    if is_paid:
        original_name = job.get("original_filename", "file")
        download_filename = f"{original_name}_shopify.csv"

        try:
            s3_key = f"output_{job_id}.csv"
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': AWS_BUCKET_NAME,
                    'Key': s3_key,
                    'ResponseContentDisposition': f'attachment; filename="{download_filename}"'
                },
                ExpiresIn=300
            )
            return RedirectResponse(url=presigned_url)
        except Exception as e:
            print(f"[S3 ERROR] Presigned URL failed for job {job_id}: {e}")
            if os.path.exists(job["output_path"]):
                return FileResponse(job["output_path"], filename=download_filename)

    raise HTTPException(status_code=402, detail="Payment required.")

# --- JOB HISTORY ---
@app.get("/my-history")
async def get_history(current_user: str = Depends(auth.get_current_user)):
    jobs = database.get_user_history(current_user)
    return {"jobs": jobs}

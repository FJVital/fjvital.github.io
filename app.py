from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import io
import os
import uuid
import stripe
import time

from orchestrator import run_orchestrator
import database
import auth

app = FastAPI()

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

# STRIPE SETUP (Live Keys)
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY")
if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY
    print(f"[STARTUP] Stripe SECRET key prefix: {STRIPE_SECRET_KEY[:8]}...")
else:
    print("[STARTUP] WARNING: STRIPE_SECRET_KEY missing.")

UPLOAD_DIR = "temp_uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.get("/")
def health_check():
    return {"status": "Schema-Sync Live"}

# --- AUTHENTICATION ---

class LoginRequest(BaseModel):
    email: str

@app.post("/token")
async def login(req: LoginRequest):
    email = req.email.strip().lower()
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
                
            # EXTRACT PREVIEW DATA
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

# --- FILE RETRIEVAL ---

@app.post("/confirm-payment/{job_id}")
async def confirm_payment(job_id: str, current_user: str = Depends(auth.get_current_user)):
    job = database.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    database.update_job_status(job_id, "paid")
    return {"status": "success", "download_url": f"{app.title}/download/{job_id}"} 

@app.get("/download/{job_id}")
async def download_file(job_id: str):
    job = database.get_job(job_id)
    
    if not job or job.get("status") != "paid":
        raise HTTPException(status_code=403, detail="File not paid for or does not exist.")

    output_path = job["output_path"]
    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="File not found on server.")

    # Generates a presigned URL valid for 1 hour
    download_url = database.generate_presigned_url(output_path)
    
    if download_url:
        return {"download_url": download_url}
    else:
        raise HTTPException(status_code=500, detail="Failed to generate download link.")

@app.get("/my-history")
async def get_history(current_user: str = Depends(auth.get_current_user)):
    jobs = database.get_user_history(current_user)
    return {"jobs": jobs}

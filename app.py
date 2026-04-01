import os
import uuid
import csv
import stripe
import boto3
from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel
from typing import List

import database
import auth

app = FastAPI()

# --- 1. HARDENED CORS CONFIGURATION ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://fjvital.github.io"], # Locks it down to your frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 2. GLOBAL PREFLIGHT CATCHER ---
# This guarantees the server always says "YES" to the browser's security check
@app.options("/{path:path}")
async def preflight_handler():
    return Response(status_code=200)

# --- CLOUD ENVIRONMENT KEYS ---
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# --- AWS S3 CONFIGURATION ---
AWS_BUCKET_NAME = os.environ.get("AWS_BUCKET_NAME", "schema-engine-bucket-1")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")

s3_client = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
)

# DELAYED IMPORT TO AVOID CIRCULAR DEPENDENCY
from orchestrator import run_orchestrator

# LOCAL STORAGE (Temp buffer before AWS)
UPLOAD_DIR = "vault"
if not os.path.exists(UPLOAD_DIR): 
    os.makedirs(UPLOAD_DIR)

# HEALTH CHECK
@app.get("/")
async def root(): 
    return {"status": "Schema-Sync Live"}

# --- AUTHENTICATION & AUTO-REGISTER ---
class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/token")
async def login(data: LoginRequest):
    user = database.get_user(data.username)
    
    # SAFETY NET: Auto-register if Render wiped the database
    if not user:
        print(f"User {data.username} not found. Auto-registering to bypass Render memory wipe...")
        database.create_user(data.username, auth.get_password_hash(data.password))
        user = database.get_user(data.username)
        
        # Re-create Stripe customer profile
        try:
            customer = stripe.Customer.create(email=user["username"])
            database.update_stripe_customer_id(user["username"], customer.id)
            user["stripe_customer_id"] = customer.id
        except Exception as e:
            print(f"Stripe Error during Auto-Register: {e}")

    # Verify Password
    if not auth.verify_password(data.password, user["hashed_password"]):
        raise HTTPException(status_code=400, detail="Incorrect credentials")
        
    access_token = auth.create_access_token(data={"sub": user["username"]})
    return {"access_token": access_token, "token_type": "bearer"}

# --- PIPELINE ENGINE ---
@app.post("/quote")
async def get_quote(file: UploadFile = File(...), current_user: str = Depends(auth.get_current_user)):
    job_id = str(uuid.uuid4())
    input_path = os.path.join(UPLOAD_DIR, f"input_{job_id}.csv")
    output_path = os.path.join(UPLOAD_DIR, f"output_{job_id}.csv")

    # Save uploaded file locally
    with open(input_path, "wb") as f: 
        f.write(await file.read())
    
    # Count Rows
    row_count = 0
    with open(input_path, "r", encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        next(reader, None)
        for _ in reader: row_count += 1

    # Run AI Pipeline
    if run_orchestrator(input_path, output_path):
        # Upload successfully processed file to AWS S3
        try:
            s3_client.upload_file(output_path, AWS_BUCKET_NAME, f"output_{job_id}.csv")
            print(f"AWS S3: Successfully vaulted output_{job_id}.csv")
        except Exception as e: 
            print(f"AWS S3 UPLOAD FAILED: {e}")
    else:
        raise HTTPException(status_code=500, detail="AI Orchestrator failed to process file.")
    
    # Calculate Price & Record Job
    total_price = max(5.00, row_count * 0.01111)
    database.create_job(job_id, current_user, input_path, output_path, int(total_price * 100))

    # Generate Preview Data
    preview_data = []
    headers = []
    with open(output_path, "r", encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        headers = next(reader)
        for i, row in enumerate(reader):
            if i < 20: preview_data.append(row)
            else: break
    
    return {
        "job_id": job_id, 
        "rows": row_count, 
        "price": total_price, 
        "headers": headers, 
        "preview": preview_data
    }

# --- PAYMENT PROCESSING ---
@app.post("/checkout/{job_id}")
async def auto_charge(job_id: str, current_user: str = Depends(auth.get_current_user)):
    job = database.get_job(job_id)
    user = database.get_user(current_user)
    
    if not job or not user: 
        raise HTTPException(status_code=404, detail="Job or User not found.")
        
    try:
        payment_methods = stripe.PaymentMethod.list(customer=user["stripe_customer_id"], type="card")
        if not payment_methods.data:
            raise HTTPException(status_code=400, detail="No card on file.")
            
        intent = stripe.PaymentIntent.create(
            amount=job["price"],
            currency='usd',
            customer=user["stripe_customer_id"],
            payment_method=payment_methods.data[0].id,
            off_session=True,
            confirm=True,
            metadata={"job_id": job_id}
        )
        database.mark_job_paid(job_id)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- SECURE DOWNLOAD ---
@app.get("/download/{job_id}")
async def download(job_id: str, token: str = None):
    # Check token from URL query string
    user = auth.get_user_from_token(token)
    if not user: 
        raise HTTPException(status_code=401, detail="Unauthorized token.")

    job = database.get_job(job_id)
    if job and job["paid"]:
        try:
            s3_key = f"output_{job_id}.csv"
            # Generate link that forces a 'Download' behavior in the browser
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': AWS_BUCKET_NAME, 
                    'Key': s3_key,
                    'ResponseContentDisposition': f'attachment; filename="schema_sync_{job_id}.csv"'
                }, 
                ExpiresIn=300
            )
            return RedirectResponse(url=presigned_url)
        except Exception as e:
            print(f"Presigned URL Error: {e}")
            # Fallback to local if AWS fails
            if os.path.exists(job["output_path"]):
                return FileResponse(job["output_path"], filename=f"schema_sync_fallback_{job_id}.csv")
            
    raise HTTPException(status_code=402, detail="Payment required or file missing.")

# --- DASHBOARD HISTORY ---
@app.get("/my-history")
async def my_history(current_user: str = Depends(auth.get_current_user)):
    history = database.get_user_history(current_user)
    return {"history": history}

# --- AWS DIAGNOSTIC PROBE ---
@app.get("/test-aws")
async def test_aws():
    try:
        s3_client.list_objects_v2(Bucket=AWS_BUCKET_NAME)
        return {"status": "SUCCESS", "message": "AWS is connected and the vault is open!"}
    except Exception as e: 
        return {"status": "BLOCKED", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)

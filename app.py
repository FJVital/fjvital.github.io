import os
import uuid
import csv
import stripe
import boto3
from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import List

import database
import auth

app = FastAPI()

# --- 1. HARDENED CORS CONFIGURATION ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://fjvital.github.io"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 2. GLOBAL PREFLIGHT CATCHER ---
@app.options("/{path:path}")
async def preflight_handler():
    return Response(status_code=200)

# --- CLOUD ENVIRONMENT KEYS ---
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
STRIPE_PUBLISHABLE_KEY = os.environ.get("STRIPE_PUBLISHABLE_KEY")

# --- STARTUP AUDIT LOG (verify key identity on every deploy) ---
_secret_prefix = (stripe.api_key or "")[:12]
_pub_prefix = (STRIPE_PUBLISHABLE_KEY or "")[:12]
print(f"[STARTUP] Stripe SECRET key prefix:      {_secret_prefix}...")
print(f"[STARTUP] Stripe PUBLISHABLE key prefix: {_pub_prefix}...")
if not stripe.api_key:
    print("[STARTUP] WARNING: STRIPE_SECRET_KEY is not set!")
if not STRIPE_PUBLISHABLE_KEY:
    print("[STARTUP] WARNING: STRIPE_PUBLISHABLE_KEY is not set!")

# --- AWS S3 CONFIGURATION ---
AWS_BUCKET_NAME = os.environ.get("AWS_BUCKET_NAME", "schema-engine-bucket-1")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")

s3_client = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
)

from orchestrator import run_orchestrator

UPLOAD_DIR = "vault"
if not os.path.exists(UPLOAD_DIR): 
    os.makedirs(UPLOAD_DIR)

# --- HEALTH CHECK ---
@app.get("/")
async def root(): 
    return {"status": "Schema-Sync Live"}

# --- CONFIG ENDPOINT (serves publishable key to frontend safely) ---
@app.get("/config")
async def get_config():
    """
    Returns the Stripe Publishable Key to the frontend.
    This keeps the key in one place (Render env vars) instead of hardcoded in index.html.
    The publishable key is NOT a secret — safe to expose via this endpoint.
    """
    if not STRIPE_PUBLISHABLE_KEY:
        raise HTTPException(status_code=500, detail="Stripe publishable key not configured on server.")
    return {"publishable_key": STRIPE_PUBLISHABLE_KEY}

# --- AUTHENTICATION & AUTO-REGISTER ---
@app.post("/token")
async def login(data: OAuth2PasswordRequestForm = Depends()):
    user = database.get_user(data.username)
    
    if not user:
        print(f"User {data.username} not found. Auto-registering...")
        database.create_user(data.username, auth.get_password_hash(data.password))
        user = database.get_user(data.username)
        
        try:
            customer = stripe.Customer.create(email=user["username"])
            database.update_stripe_customer_id(user["username"], customer.id)
            user["stripe_customer_id"] = customer.id
        except Exception as e:
            print(f"Stripe Error during Auto-Register: {e}")

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

    with open(input_path, "wb") as f: 
        f.write(await file.read())
    
    row_count = 0
    with open(input_path, "r", encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        next(reader, None)
        for _ in reader: row_count += 1

    if run_orchestrator(input_path, output_path):
        try:
            s3_client.upload_file(output_path, AWS_BUCKET_NAME, f"output_{job_id}.csv")
        except Exception as e: 
            print(f"AWS S3 UPLOAD FAILED: {e}")
    else:
        raise HTTPException(status_code=500, detail="AI Orchestrator failed.")
    
    total_price = max(5.00, row_count * 0.01111)
    database.create_job(job_id, current_user, input_path, output_path, int(total_price * 100))

    preview_data = []
    headers = []
    with open(output_path, "r", encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        headers = next(reader)
        for i, row in enumerate(reader):
            if i < 20: preview_data.append(row)
            else: break
    
    return {"job_id": job_id, "rows": row_count, "price": total_price, "headers": headers, "preview": preview_data}

# --- PAYMENT PROCESSING ---

# STEP 1: Ask Stripe for a secure Payment Intent
@app.get("/create-payment-intent/{job_id}")
async def create_payment_intent(job_id: str, current_user: str = Depends(auth.get_current_user)):
    job = database.get_job(job_id)
    user = database.get_user(current_user)
    
    if not job or not user: 
        raise HTTPException(status_code=404, detail="Job or User not found.")
        
    try:
        intent = stripe.PaymentIntent.create(
            amount=job["price"],
            currency='usd',
            customer=user["stripe_customer_id"],
        )
        print(f"[STRIPE] PaymentIntent created: {intent.id} for job {job_id} | amount: {job['price']}")
        return {"client_secret": intent.client_secret}
    except Exception as e:
        print(f"[STRIPE ERROR] Failed to create PaymentIntent for job {job_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))

class VerifyRequest(BaseModel):
    payment_intent_id: str

# STEP 2: Verify the card worked before unlocking the file
@app.post("/verify-payment/{job_id}")
async def verify_payment(job_id: str, req: VerifyRequest, current_user: str = Depends(auth.get_current_user)):
    job = database.get_job(job_id)
    if not job: raise HTTPException(status_code=404, detail="Job not found")

    try:
        intent = stripe.PaymentIntent.retrieve(req.payment_intent_id)
        print(f"[STRIPE] Verifying PaymentIntent {intent.id} | status: {intent.status}")
        if intent.status == 'succeeded':
            database.mark_job_paid(job_id)
            return {"status": "success"}
        else:
            raise HTTPException(status_code=400, detail="Payment failed or incomplete")
    except Exception as e:
        print(f"[STRIPE ERROR] Failed to verify PaymentIntent {req.payment_intent_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# --- SECURE DOWNLOAD ---
@app.get("/download/{job_id}")
async def download(job_id: str, token: str = None):
    user = auth.get_user_from_token(token)
    if not user: 
        raise HTTPException(status_code=401, detail="Unauthorized token.")

    job = database.get_job(job_id)
    if job and job["paid"]:
        try:
            s3_key = f"output_{job_id}.csv"
            presigned_url = s3_client.generate_presigned_url('get_object',
                Params={'Bucket': AWS_BUCKET_NAME, 'Key': s3_key, 'ResponseContentDisposition': f'attachment; filename="schema_sync_{job_id}.csv"'}, 
                ExpiresIn=300)
            return RedirectResponse(url=presigned_url)
        except Exception as e:
            print(f"[S3 ERROR] Presigned URL failed for job {job_id}: {e}")
            if os.path.exists(job["output_path"]):
                return FileResponse(job["output_path"], filename=f"schema_sync_fallback_{job_id}.csv")
            
    raise HTTPException(status_code=402, detail="Payment required.")

# --- JOB HISTORY ---
@app.get("/my-history")
async def my_history(current_user: str = Depends(auth.get_current_user)):
    history = database.get_user_history(current_user)
    return {"history": history}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)

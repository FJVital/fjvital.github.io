import os
import uuid
import csv
import stripe
from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List

# INTERNAL MODULES
import database
import auth

# INITIALIZE APP
app = FastAPI()

# MASTER CORS CONFIGURATION
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=False, 
    allow_methods=["*"],
    allow_headers=["*"],
)

# CLOUD ENVIRONMENT KEYS
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# DELAYED IMPORT
from orchestrator import run_orchestrator

# STORAGE
UPLOAD_DIR = "vault"
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)

# HEALTH CHECK
@app.get("/")
@app.head("/")
async def root():
    return {"status": "Schema-Sync Backend is Live"}

class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/token")
async def login(data: LoginRequest):
    user = database.get_user(data.username)
    if not user or not auth.verify_password(data.password, user["hashed_password"]):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    
    if not user.get("stripe_customer_id"):
        try:
            customer = stripe.Customer.create(email=user["username"])
            database.update_stripe_customer_id(user["username"], customer.id)
            user["stripe_customer_id"] = customer.id
        except Exception as e:
            raise HTTPException(status_code=500, detail="Billing system error.")

    access_token = auth.create_access_token(data={"sub": user["username"]})
    return {"access_token": access_token, "token_type": "bearer"}

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
        for _ in reader:
            row_count += 1

    success = run_orchestrator(input_path, output_path)
    if not success:
        raise HTTPException(status_code=500, detail="AI Orchestrator failed.")

    preview_data = []
    headers = []
    with open(output_path, "r", encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        headers = next(reader)
        for i, row in enumerate(reader):
            if i < 20:
                preview_data.append(row)
            else:
                break

    total_price = max(5.00, row_count * 0.01111)
    
    database.create_job(job_id, input_path, output_path, int(total_price * 100))

    return {
        "job_id": job_id,
        "rows_detected": row_count,
        "total_price_usd": total_price,
        "preview_headers": headers,
        "preview_data": preview_data
    }

@app.post("/vault-card")
async def vault_card(current_user: str = Depends(auth.get_current_user)):
    user = database.get_user(current_user)
    if not user or not user.get("stripe_customer_id"):
        raise HTTPException(status_code=400, detail="No billing profile found.")
    
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            mode='setup',
            customer=user["stripe_customer_id"],
            success_url="https://fjvital.github.io/schema-sync-engine/?setup=success",
            cancel_url="https://fjvital.github.io/schema-sync-engine/?setup=cancel",
        )
        return {"vault_url": session.url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/checkout/{job_id}")
async def auto_charge(job_id: str, current_user: str = Depends(auth.get_current_user)):
    job = database.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    user = database.get_user(current_user)
    if not user or not user.get("stripe_customer_id"):
        raise HTTPException(status_code=400, detail="No billing profile found.")
        
    try:
        payment_methods = stripe.PaymentMethod.list(
            customer=user["stripe_customer_id"],
            type="card",
        )
        if not payment_methods.data:
            raise HTTPException(status_code=400, detail="No card on file. Please add a card via Express Billing.")
            
        payment_method_id = payment_methods.data[0].id
        
        intent = stripe.PaymentIntent.create(
            amount=job["price"],
            currency='usd',
            customer=user["stripe_customer_id"],
            payment_method=payment_method_id,
            off_session=True,
            confirm=True,
            metadata={"job_id": job_id}
        )
        
        database.mark_job_paid(job_id)
        return {"status": "success"}
        
    except stripe.error.CardError as e:
        print(f"STRIPE CARD ERROR: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Charge declined: {e.user_message}")
    except stripe.error.StripeError as e:
        print(f"STRIPE API ERROR: {str(e)}")
        # This prevents the 500 error and sends the real Stripe reason to the frontend
        raise HTTPException(status_code=400, detail=f"Stripe configuration error: {str(e)}")
    except Exception as e:
        print(f"SYSTEM ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal System Error: {str(e)}")

@app.post("/stripe-webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET")

    if not webhook_secret:
        return {"status": "unconfigured"}

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, webhook_secret
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid Stripe signature")

    if event["type"] == "payment_intent.succeeded":
        intent = event["data"]["object"]
        job_id = intent.get("metadata", {}).get("job_id")
        
        if job_id:
            database.mark_job_paid(job_id)
            print(f"WEBHOOK SECURE: Job {job_id} permanently unlocked.")

    return {"status": "success"}

@app.post("/verify-payment/{job_id}")
async def verify(job_id: str, current_user: str = Depends(auth.get_current_user)):
    job = database.get_job(job_id)
    if job and job["paid"]:
        return {"status": "verified"}
    raise HTTPException(status_code=404)

@app.get("/download/{job_id}")
async def download(job_id: str, current_user: str = Depends(auth.get_current_user)):
    job = database.get_job(job_id)
    if job and job["paid"]:
        return FileResponse(job["output_path"], filename="shopify_ready_final.csv")
    raise HTTPException(status_code=402, detail="Payment required")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)
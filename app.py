import os
import uuid
import csv
import stripe
from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import OAuth2PasswordRequestForm
from typing import List

# INTERNAL MODULES
import database
import auth

# INITIALIZE APP
app = FastAPI()

# MASTER CORS CONFIGURATION (MAXIMUM WHITELIST)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "[https://fjvital.github.io](https://fjvital.github.io)",
        "[http://fjvital.github.io](http://fjvital.github.io)",
        "http://localhost:8000",
        "http://localhost:10000",
        "http://localhost:5500",
        "[http://127.0.0.1:5500](http://127.0.0.1:5500)",
        "null" # Allows testing locally by double-clicking the HTML file
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# CLOUD ENVIRONMENT KEYS
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# DELAYED IMPORT: Prevents NameError by ensuring keys are loaded first
from orchestrator import run_orchestrator

# STORAGE
UPLOAD_DIR = "vault"
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)

# VOLATILE JOB DATABASE
jobs = {}

@app.get("/")
async def root():
    """Health check endpoint to ensure server is live."""
    return {"status": "Schema-Sync Backend is Live"}

@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = database.get_user(form_data.username)
    if not user or not auth.verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    
    # PHASE 1 AUTO-BILLING: Ensure user has a Stripe Customer profile
    if not user.get("stripe_customer_id"):
        try:
            customer = stripe.Customer.create(email=user["username"])
            database.update_stripe_customer_id(user["username"], customer.id)
            user["stripe_customer_id"] = customer.id
            print(f"Created Stripe Customer: {customer.id}")
        except Exception as e:
            print(f"Stripe Customer Creation Failed: {e}")
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
        raise HTTPException(status_code=500, detail="AI Orchestrator failed to process CSV.")

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

    # UPDATED PRICING MARGIN
    total_price = max(5.00, row_count * 0.01111)

    jobs[job_id] = {
        "input_path": input_path,
        "output_path": output_path,
        "price": int(total_price * 100),
        "paid": False
    }

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
            success_url="[https://fjvital.github.io/schema-sync-engine/?setup=success](https://fjvital.github.io/schema-sync-engine/?setup=success)",
            cancel_url="[https://fjvital.github.io/schema-sync-engine/?setup=cancel](https://fjvital.github.io/schema-sync-engine/?setup=cancel)",
        )
        return {"vault_url": session.url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/checkout/{job_id}")
async def auto_charge(job_id: str, current_user: str = Depends(auth.get_current_user)):
    """PHASE 3: The 1-Click Auto-Charge Engine"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    user = database.get_user(current_user)
    if not user or not user.get("stripe_customer_id"):
        raise HTTPException(status_code=400, detail="No billing profile found. Please add a card.")
        
    job = jobs[job_id]
    
    try:
        # Fetch the user's vaulted card from Stripe
        payment_methods = stripe.PaymentMethod.list(
            customer=user["stripe_customer_id"],
            type="card",
        )
        
        if not payment_methods.data:
            raise HTTPException(status_code=400, detail="No card on file. Please use the Express Billing setup first.")
            
        payment_method_id = payment_methods.data[0].id
        
        # Fire the silent Auto-Charge
        intent = stripe.PaymentIntent.create(
            amount=job["price"],
            currency='usd',
            customer=user["stripe_customer_id"],
            payment_method=payment_method_id,
            off_session=True, # Background charge
            confirm=True,     # Instantly authorize and capture
        )
        
        # Unlock the file
        jobs[job_id]["paid"] = True
        return {"status": "success"}
        
    except stripe.error.CardError as e:
        raise HTTPException(status_code=400, detail=f"Charge declined: {e.user_message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal billing error.")

@app.post("/verify-payment/{job_id}")
async def verify(job_id: str, current_user: str = Depends(auth.get_current_user)):
    if job_id in jobs:
        jobs[job_id]["paid"] = True
        return {"status": "verified"}
    raise HTTPException(status_code=404)

@app.get("/download/{job_id}")
async def download(job_id: str, current_user: str = Depends(auth.get_current_user)):
    if job_id in jobs and jobs[job_id]["paid"]:
        return FileResponse(jobs[job_id]["output_path"], filename="shopify_ready_final.csv")
    raise HTTPException(status_code=402, detail="Payment required")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)
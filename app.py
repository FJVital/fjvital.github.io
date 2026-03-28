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

# MASTER CORS CONFIGURATION
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# CLOUD ENVIRONMENT KEYS
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# DELAYED IMPORT: This prevents the NameError by ensuring keys are loaded first
from orchestrator import run_orchestrator

# STORAGE
UPLOAD_DIR = "vault"
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)

# VOLATILE JOB DATABASE
jobs = {}

@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = database.get_user(form_data.username)
    if not user or not auth.verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    
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

    # Execute the AI Mapping
    success = run_orchestrator(input_path, output_path)
    if not success:
        raise HTTPException(status_code=500, detail="AI Orchestrator failed to process CSV.")

    # Generate Preview
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

    total_price = max(5.00, row_count * 0.01)

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

@app.post("/checkout/{job_id}")
async def create_checkout(job_id: str, current_user: str = Depends(auth.get_current_user)):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[{
                'price_data': {
                    'currency': 'usd',
                    'product_data': {'name': 'CSV Schema-Sync Processing'},
                    'unit_amount': job["price"],
                },
                'quantity': 1,
            }],
            mode='payment',
            success_url=f"https://fjvital.github.io/schema-sync-engine/?status=success&jobId={job_id}",
            cancel_url=f"https://fjvital.github.io/schema-sync-engine/?status=cancel",
        )
        return {"checkout_url": session.url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
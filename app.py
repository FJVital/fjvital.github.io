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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=False, 
    allow_methods=["*"],
    allow_headers=["*"],
)

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# AWS CONFIG
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
if not os.path.exists(UPLOAD_DIR): os.makedirs(UPLOAD_DIR)

@app.get("/")
async def root(): return {"status": "Schema-Sync Live"}

class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/token")
async def login(data: LoginRequest):
    user = database.get_user(data.username)
    if not user or not auth.verify_password(data.password, user["hashed_password"]):
        raise HTTPException(status_code=400, detail="Incorrect credentials")
    access_token = auth.create_access_token(data={"sub": user["username"]})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/quote")
async def get_quote(file: UploadFile = File(...), current_user: str = Depends(auth.get_current_user)):
    job_id = str(uuid.uuid4())
    input_path = os.path.join(UPLOAD_DIR, f"input_{job_id}.csv")
    output_path = os.path.join(UPLOAD_DIR, f"output_{job_id}.csv")

    with open(input_path, "wb") as f: f.write(await file.read())
    
    row_count = 0
    with open(input_path, "r", encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        next(reader, None)
        for _ in reader: row_count += 1

    if run_orchestrator(input_path, output_path):
        try:
            s3_client.upload_file(output_path, AWS_BUCKET_NAME, f"output_{job_id}.csv")
        except Exception as e: print(f"S3 Error: {e}")
    
    total_price = max(5.00, row_count * 0.01111)
    database.create_job(job_id, current_user, input_path, output_path, int(total_price * 100))

    preview_data = []
    with open(output_path, "r", encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        headers = next(reader)
        for i, row in enumerate(reader):
            if i < 20: preview_data.append(row)
    
    return {"job_id": job_id, "rows": row_count, "price": total_price, "headers": headers, "preview": preview_data}

@app.get("/download/{job_id}")
async def download(job_id: str, token: str = None):
    # Check token from URL query string
    user = auth.get_user_from_token(token)
    if not user: raise HTTPException(status_code=401)

    job = database.get_job(job_id)
    if job and job["paid"]:
        try:
            s3_key = f"output_{job_id}.csv"
            # Generate link that forces a 'Download' behavior in the browser
            presigned_url = s3_client.generate_presigned_url('get_object',
                Params={
                    'Bucket': AWS_BUCKET_NAME, 
                    'Key': s3_key,
                    'ResponseContentDisposition': f'attachment; filename="schema_sync_{job_id}.csv"'
                }, ExpiresIn=300)
            return RedirectResponse(url=presigned_url)
        except:
            return FileResponse(job["output_path"], filename="fallback.csv")
    raise HTTPException(status_code=402)

# --- AWS PROBE ---
@app.get("/test-aws")
async def test_aws():
    try:
        s3_client.list_objects_v2(Bucket=AWS_BUCKET_NAME)
        return {"status": "SUCCESS"}
    except Exception as e: return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)
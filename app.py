import os
import uuid
import stripe
import re
import csv
import shutil
from fastapi import FastAPI, Depends, HTTPException, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

import database
import auth

app = FastAPI()

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://flashfix.io",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5500",
        "http://127.0.0.1:5500",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Stripe
# ---------------------------------------------------------------------------
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

# ---------------------------------------------------------------------------
# Upload directory
# ---------------------------------------------------------------------------
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------
class JobRequest(BaseModel):
    file_url: str
    target_platform: str = "shopify"

class GuestLoginRequest(BaseModel):
    username: str
    password: str

# ---------------------------------------------------------------------------
# Auth endpoints
# ---------------------------------------------------------------------------
@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = auth.authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token = auth.create_access_token(data={"sub": user["username"]})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/guest-token")
async def guest_login(body: GuestLoginRequest):
    GUEST_USERNAME = "guest"
    GUEST_PASSWORD = "guest"
    if body.username != GUEST_USERNAME or body.password != GUEST_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid guest credentials")
    guest = database.get_user(GUEST_USERNAME)
    if not guest:
        hashed = auth.get_password_hash(GUEST_PASSWORD)
        database.create_user(GUEST_USERNAME, hashed)
    access_token = auth.create_access_token(data={"sub": GUEST_USERNAME})
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/register")
async def register(body: GuestLoginRequest):
    existing = database.get_user(body.username)
    if existing:
        raise HTTPException(status_code=400, detail="Username already taken")
    hashed = auth.get_password_hash(body.password)
    database.create_user(body.username, hashed)
    access_token = auth.create_access_token(data={"sub": body.username})
    return {"access_token": access_token, "token_type": "bearer"}

# ---------------------------------------------------------------------------
# Core endpoints
# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    return {"status": "active", "service": "flashfix-engine"}

@app.get("/me")
async def get_me(current_user: str = Depends(auth.get_current_user)):
    user = database.get_user(current_user)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return {
        "username": user["username"],
        "credits": user.get("credits", 0),
    }

@app.post("/quote")
async def quote(
    file: UploadFile = File(...),
    target_platform: str = Form(default="shopify"),
    current_user: str = Depends(auth.get_current_user),
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted")

    job_id = str(uuid.uuid4())
    safe_filename = f"{job_id}_{file.filename}"
    input_path = os.path.join(UPLOAD_DIR, safe_filename)

    with open(input_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    preview_rows, column_count, row_count = _preview_csv(input_path, target_platform)
    
    # Filename format cleaned up per previous request
    output_filename = f"shopify_{file.filename}"
    output_path = os.path.join(UPLOAD_DIR, output_filename)
    _transform_csv(input_path, output_path, target_platform)
    download_url = f"/files/{output_filename}"

    database.create_job(
        job_id=job_id,
        username=current_user,
        input_path=input_path,
        output_path=output_path,
        price=500,
        original_filename=file.filename,
        preview_data=str(preview_rows),
        download_url=download_url,
    )

    user_record = database.get_user(current_user)
    credits_available = user_record.get("credits", 0) if user_record else 0

    return {
        "job_id": job_id,
        "status": "ready",
        "original_filename": file.filename,
        "rows_detected": row_count,
        "columns_detected": column_count,
        "price_cents": 500,
        "preview": preview_rows,
        "credits": credits_available,
        "message": f"File processed for {target_platform}. Use a credit or pay $5 to download.",
    }

# ---------------------------------------------------------------------------
# SMART CSV ENGINE (Auto-Handles, Variant Grouping, Defaults)
# ---------------------------------------------------------------------------
def _generate_handle(title: str) -> str:
    if not title:
        return ""
    # Converts "Cool Shirt!" to "cool-shirt"
    return re.sub(r'[^a-z0-9]+', '-', str(title).lower()).strip('-')

def _get_standard_headers():
    return [
        "Handle", "Title", "Body (HTML)", "Vendor", "Product Category", "Type", "Tags",
        "Published", "Option1 Name", "Option1 Value", "Option2 Name", "Option2 Value",
        "Option3 Name", "Option3 Value", "Variant SKU", "Variant Grams",
        "Variant Inventory Tracker", "Variant Inventory Qty", "Variant Inventory Policy",
        "Variant Fulfillment Service", "Variant Price", "Variant Compare At Price",
        "Variant Requires Shipping", "Variant Taxable", "Variant Barcode",
        "Image Src", "Image Position", "Image Alt Text", "Gift Card",
        "SEO Title", "SEO Description", "Variant Image", "Variant Weight Unit",
        "Variant Tax Code", "Cost per item", "Status"
    ]

def _get_shopify_map():
    return {
        "product name": "Title", "name": "Title", "title": "Title", "item name": "Title",
        "sku": "Variant SKU", "item number": "Variant SKU", "part number": "Variant SKU",
        "price": "Variant Price", "cost": "Variant Price", "msrp": "Variant Price", "regular price": "Variant Price",
        "qty": "Variant Inventory Qty", "quantity": "Variant Inventory Qty", "stock": "Variant Inventory Qty", "inventory": "Variant Inventory Qty",
        "description": "Body (HTML)", "desc": "Body (HTML)", "details": "Body (HTML)",
        "image": "Image Src", "image url": "Image Src", "photo": "Image Src", "picture": "Image Src", "image 1": "Image Src",
        "weight": "Variant Grams", "brand": "Vendor", "vendor": "Vendor", "manufacturer": "Vendor",
        "upc": "Variant Barcode", "barcode": "Variant Barcode", "ean": "Variant Barcode",
        "category": "Product Category", "type": "Type",
        "tags": "Tags", "keywords": "Tags",
        "status": "Status", "handle": "Handle", "url handle": "Handle"
    }

def _process_single_row(row, mapped_headers, last_handle, last_title):
    out_row = {h: "" for h in _get_standard_headers()}
    
    # Map incoming data
    for k, v in row.items():
        if k is None: continue
        mapped_key = mapped_headers.get(k.strip().lower(), k.strip())
        if mapped_key in out_row:
            out_row[mapped_key] = str(v).strip()
    
    current_title = out_row.get("Title", "")
    current_handle = out_row.get("Handle", "")

    # Grouping Logic: Generate Handle and Inherit for Variants
    if current_handle:
        last_handle = current_handle
        last_title = current_title
    elif current_title:
        last_handle = _generate_handle(current_title)
        last_title = current_title
        out_row["Handle"] = last_handle
    else:
        # It's a variant of the previous row! Inherit handle and title.
        out_row["Handle"] = last_handle
        out_row["Title"] = last_title

    # Smart Shopify Defaults
    if not out_row.get("Vendor"): out_row["Vendor"] = "FlashFix Defaults"
    if not out_row.get("Status"): out_row["Status"] = "active"
    if not out_row.get("Published"): out_row["Published"] = "TRUE"
    if not out_row.get("Variant Inventory Tracker"): out_row["Variant Inventory Tracker"] = "shopify"
    if not out_row.get("Variant Inventory Policy"): out_row["Variant Inventory Policy"] = "deny"
    if not out_row.get("Variant Fulfillment Service"): out_row["Variant Fulfillment Service"] = "manual"
    
    if not out_row.get("Variant Inventory Qty"): out_row["Variant Inventory Qty"] = "1"
    
    return out_row, last_handle, last_title

def _preview_csv(path: str, platform: str, max_rows: int = 5) -> tuple:
    preview_rows = []
    row_count = 0
    column_count = len(_get_standard_headers())
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            mapped_headers = {h.strip().lower(): _get_shopify_map().get(h.strip().lower(), h) for h in headers if h}

            last_handle = ""
            last_title = ""

            for row in reader:
                row_count += 1
                if len(preview_rows) < max_rows:
                    out_row, last_handle, last_title = _process_single_row(row, mapped_headers, last_handle, last_title)
                    preview_rows.append(out_row)
    except Exception as e:
        print("Preview Error:", e)
    return preview_rows, column_count, row_count

def _transform_csv(input_path: str, output_path: str, platform: str):
    try:
        with open(input_path, newline="", encoding="utf-8-sig") as fin:
            reader = csv.DictReader(fin)
            headers = reader.fieldnames or []
            mapped_headers = {h.strip().lower(): _get_shopify_map().get(h.strip().lower(), h) for h in headers if h}

            last_handle = ""
            last_title = ""
            rows = []
            
            for row in reader:
                out_row, last_handle, last_title = _process_single_row(row, mapped_headers, last_handle, last_title)
                rows.append(out_row)

        if rows:
            with open(output_path, "w", newline="", encoding="utf-8") as fout:
                writer = csv.DictWriter(fout, fieldnames=_get_standard_headers())
                writer.writeheader()
                writer.writerows(rows)
    except Exception:
        open(output_path, "w").close()

@app.post("/create-job")
async def create_job(request: JobRequest, current_user: str = Depends(auth.get_current_user)):
    job_id = database.create_new_job(current_user, request.file_url)
    return {"job_id": job_id, "status": "processing"}

@app.get("/job-status/{job_id}")
async def get_job_status(job_id: str, current_user: str = Depends(auth.get_current_user)):
    job = database.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    owner = job.get("user_id") or job.get("username")
    if owner != current_user:
        raise HTTPException(status_code=403, detail="Not authorized to view this job")
    return {
        "job_id": job_id,
        "status": job.get("status"),
        "preview": job.get("preview_data"),
    }

# ---------------------------------------------------------------------------
# Payment & Credit endpoints
# ---------------------------------------------------------------------------
@app.post("/use-credit/{job_id}")
async def use_credit(job_id: str, current_user: str = Depends(auth.get_current_user)):
    user = database.get_user(current_user)
    job = database.get_job(job_id)
    if not user or not job:
        raise HTTPException(status_code=404, detail="Data not found.")
    if user.get("credits", 0) > 0:
        database.decrement_credit(current_user)
        database.mark_job_paid(job_id)
        return {
            "status": "success",
            "credits_remaining": user["credits"] - 1,
            "download_url": job.get("download_url"),
        }
    else:
        raise HTTPException(status_code=402, detail="No free credits remaining.")

@app.post("/create-checkout-session/{job_id}")
async def create_checkout_session(job_id: str, current_user: str = Depends(auth.get_current_user)):
    job = database.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{
                "price_data": {
                    "currency": "usd",
                    "product_data": {"name": f"flashfix.io — CSV Format ({job.get('original_filename', 'file')})"},
                    "unit_amount": 500,
                },
                "quantity": 1,
            }],
            mode="payment",
            success_url=f"https://flashfix.io/success?session_id={{CHECKOUT_SESSION_ID}}&job_id={job_id}",
            cancel_url="https://flashfix.io/cancel",
            client_reference_id=job_id,
            metadata={"user_id": current_user},
        )
        return {"url": session.url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/verify-payment/{job_id}")
async def verify_payment(job_id: str, request: Request, current_user: str = Depends(auth.get_current_user)):
    job = database.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    database.mark_job_paid(job_id)
    job = database.get_job(job_id)
    return {"status": "success", "download_url": job.get("download_url")}

@app.get("/download/{job_id}")
async def download_csv(job_id: str, current_user: str = Depends(auth.get_current_user)):
    job = database.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") != "paid" and not job.get("paid"):
        raise HTTPException(status_code=402, detail="Payment or credit required to download")
    download_url = job.get("download_url")
    if not download_url:
        raise HTTPException(status_code=404, detail="Download link not ready yet")
    return {"download_url": download_url}

@app.get("/history")
async def get_history(current_user: str = Depends(auth.get_current_user)):
    jobs = database.get_user_history(current_user)
    return {"history": jobs}


# This forces the browser to download the file instead of opening it as text
@app.get("/files/{filename}")
async def get_file(filename: str):
    file_path = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path=file_path, filename=filename, media_type='text/csv')

import os
import csv
import json
import time
from google import genai
from google.genai import types

# INITIALIZE AI CLIENT (new unified SDK)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if GEMINI_API_KEY:
    client = genai.Client(api_key=GEMINI_API_KEY)
    print("[STARTUP] Gemini AI client initialized via google-genai SDK.")
else:
    client = None
    print("[STARTUP] WARNING: GEMINI_API_KEY not found in environment variables.")

def run_orchestrator(input_file, output_file):
    """
    Reads a raw CSV, maps headers to Shopify format using Gemini AI (JSON mode),
    and saves the synchronized version.
    Primary model: gemini-2.5-flash (stable, high capacity)
    Fallback model: gemini-2.5-flash-lite (if primary is overloaded)
    """
    if not client:
        print("Orchestrator Error: Gemini client not initialized. GEMINI_API_KEY missing.")
        return False

    raw_data = []
    try:
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            headers = next(reader)

            # Take a sample of the first 5 rows to help the AI understand the data
            sample_rows = []
            for i, row in enumerate(reader):
                sample_rows.append(row)
                if i >= 4: break

            # Reset and read full data for the actual transformation
            f.seek(0)
            next(reader)
            raw_data = list(reader)

        shopify_headers = [
            "Handle", "Title", "Body (HTML)", "Vendor", "Type", "Tags", "Published",
            "Option1 Name", "Option1 Value", "Variant SKU", "Variant Grams", "Variant Inventory Tracker",
            "Variant Inventory Qty", "Variant Inventory Policy", "Variant Fulfillment Service",
            "Variant Price", "Variant Compare At Price", "Image Src"
        ]

        # --- UPDATED AI PROMPT: AUTO-CATEGORIZATION INJECTION ---
        prompt = (
            f"Map these input headers to the target headers based on the sample data.\n"
            f"Input headers: {headers}\n"
            f"Sample data: {sample_rows}\n"
            f"Target headers: {shopify_headers}\n\n"
            f"Return a strict JSON object where the keys are the exact Target headers.\n"
            f"For most keys, the value must be the integer index (0-based) of the matching Input header. If there is no match, use null.\n"
            f"SPECIAL INSTRUCTION FOR 'Type': You are an expert E-commerce Merchandiser. Analyze the sample data to determine what kind of products these are. Instead of an integer index, output a standard, 1-to-3 word product category string (e.g., 'Smart Watch', 'Phone Case', 'Leather Band') as the value for the 'Type' key.\n"
            f"Do not write any other text."
        )

        # MODEL CASCADE: try primary first, fall back on 503/UNAVAILABLE
        models_to_try = [
            'gemini-2.5-flash',
            'gemini-2.5-flash-lite',
        ]

        response = None
        last_error = None

        for model_name in models_to_try:
            try:
                print(f"[ORCHESTRATOR] Trying model: {model_name}")
                # Use the new SDK config to FORCE structured JSON output
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.1
                    )
                )
                print(f"[ORCHESTRATOR] Success with model: {model_name}")
                break
            except Exception as e:
                last_error = e
                err_str = str(e)
                if '503' in err_str or 'UNAVAILABLE' in err_str or 'quota' in err_str.lower():
                    print(f"[ORCHESTRATOR] {model_name} unavailable, trying next model...")
                    time.sleep(1)
                    continue
                else:
                    raise e

        if response is None:
            raise last_error

        # --- STRUCTURED JSON PARSER ---
        json_text = response.text.strip()
        mapping_dict = json.loads(json_text)
        # -----------------------------------

        # TRANSFORM AND SAVE
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(shopify_headers)

            for row in raw_data:
                new_row = []
                for target_header in shopify_headers:
                    val = mapping_dict.get(target_header)
                    
                    # --- UPDATED LOGIC: INJECT THE CATEGORY STRING ---
                    # If it's a valid integer index from the CSV
                    if isinstance(val, int) and not isinstance(val, bool) and 0 <= val < len(row):
                        new_row.append(row[val])
                    # If the AI passed us the custom String category for Type
                    elif target_header == "Type" and isinstance(val, str):
                        new_row.append(val)
                    else:
                        new_row.append("")
                        
                writer.writerow(new_row)

        return True

    except Exception as e:
        print(f"Orchestrator Error: {str(e)}")
        return False

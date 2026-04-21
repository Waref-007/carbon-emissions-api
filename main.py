from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import List, Annotated, Dict, Any
import io
import os
import requests
import pandas as pd

from engine import run_emissions_engine, preprocess_uploaded_dataframe


# =========================
# APP INITIALISATION
# =========================
app = FastAPI(
    title="Carbon Emissions API v3",
    version="3.0.0"
)


# =========================
# CORS
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://gscsustainability.co.uk",
        "https://www.gscsustainability.co.uk",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
# HEALTH
# =========================
@app.get("/")
def root():
    return {"status": "ok", "version": "v3"}


@app.get("/health")
def health():
    return {"status": "healthy"}


# =========================
# UTILS (LIGHTWEIGHT ONLY)
# =========================
def safe_str(v):
    return "" if v is None else str(v).strip()


def validate_required_fields(data: Dict[str, Any]):
    required = [
        "full_name",
        "email",
        "company_name",
        "phone_number",
        "privacy_consent",
        "contact_consent",
    ]

    missing = [r for r in required if not safe_str(data.get(r))]

    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required fields: {missing}"
        )

    if str(data.get("privacy_consent")).lower() != "true":
        raise HTTPException(status_code=400, detail="Privacy consent required")

    if str(data.get("contact_consent")).lower() != "true":
        raise HTTPException(status_code=400, detail="Contact consent required")


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def read_file(upload: UploadFile):
    filename = (upload.filename or "").lower()
    content = upload.file.read()

    if filename.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)

    elif filename.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(content), dtype=str)

    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {upload.filename}"
        )

    df = normalize_df(df)
    return preprocess_uploaded_dataframe(df)


def build_user(data: Dict[str, Any]):
    return {
        "full_name": data["full_name"],
        "email": data["email"],
        "company_name": data["company_name"],
        "phone_number": data["phone_number"],
    }


# =========================
# ENGINE WRAPPER (v3 CLEAN)
# =========================
def run_engine(activities: list) -> dict:
    """
    SINGLE SOURCE OF TRUTH:
    Engine handles:
    - emissions
    - analytics
    - data quality
    - errors
    """

    result = run_emissions_engine({"activities": activities})

    if not isinstance(result, dict):
        raise HTTPException(
            status_code=500,
            detail="Engine returned invalid response format"
        )

    return result


# =========================
# LEAD LOGGER
# =========================
def send_lead(user: dict, result: dict):
    url = os.getenv("LEAD_LOGGER_URL")
    if not url:
        return

    try:
        payload = {
            **user,
            "total_kgco2e": result.get("totals", {}).get("total_kgCO2e", 0),
            "total_tco2e": result.get("totals", {}).get("total_tCO2e", 0),
            "error_count": len(result.get("errors", [])),
            "source": "api_v3"
        }

        requests.post(url, json=payload, timeout=5)

    except Exception:
        pass  # never break API flow


# =========================
# ROUTE: PURE CALCULATION
# =========================
@app.post("/calculate")
def calculate(payload: dict):
    activities = payload.get("activities", [])

    if not isinstance(activities, list):
        raise HTTPException(status_code=400, detail="activities must be a list")

    return run_engine(activities)


# =========================
# ROUTE: UPLOAD + CALCULATE
# =========================
@app.post("/upload-calculate")
async def upload_calculate(
    files: Annotated[List[UploadFile], File(...)],
    full_name: Annotated[str, Form(...)],
    email: Annotated[str, Form(...)],
    company_name: Annotated[str, Form(...)],
    phone_number: Annotated[str, Form(...)],
    privacy_consent: Annotated[str, Form(...)],
    contact_consent: Annotated[str, Form(...)],
):
    try:
        payload = {
            "full_name": full_name,
            "email": email,
            "company_name": company_name,
            "phone_number": phone_number,
            "privacy_consent": privacy_consent,
            "contact_consent": contact_consent,
        }

        validate_required_fields(payload)

        user = build_user(payload)

        # =========================
        # FILE PROCESSING
        # =========================
        activities = []
        uploaded_files = []

        for f in files:
            df = read_file(f)

            df["source_file"] = f.filename
            df["row_index_original"] = range(1, len(df) + 1)

            activities.extend(df.to_dict(orient="records"))

            uploaded_files.append({
                "filename": f.filename,
                "rows": len(df),
                "columns": list(df.columns)
            })

        if not activities:
            raise HTTPException(status_code=400, detail="No valid data found")

        # =========================
        # ENGINE CALL
        # =========================
        result = run_engine(activities)

        # =========================
        # RESPONSE WRAP
        # =========================
        response = {
            "user": user,
            "uploaded_files": uploaded_files,
            "input_row_count": len(activities),
            **result
        }

        send_lead(user, response)

        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# ROUTE: DOWNLOAD REPORT
# =========================
@app.post("/upload-calculate-download")
async def upload_calculate_download(
    files: Annotated[List[UploadFile], File(...)],
    full_name: Annotated[str, Form(...)],
    email: Annotated[str, Form(...)],
    company_name: Annotated[str, Form(...)],
    phone_number: Annotated[str, Form(...)],
    privacy_consent: Annotated[str, Form(...)],
    contact_consent: Annotated[str, Form(...)],
):
    try:
        payload = {
            "full_name": full_name,
            "email": email,
            "company_name": company_name,
            "phone_number": phone_number,
            "privacy_consent": privacy_consent,
            "contact_consent": contact_consent,
        }

        validate_required_fields(payload)
        user = build_user(payload)

        activities = []

        for f in files:
            df = read_file(f)
            df["source_file"] = f.filename
            df["row_index_original"] = range(1, len(df) + 1)
            activities.extend(df.to_dict(orient="records"))

        result = run_engine(activities)

        response = {
            "user": user,
            "input_row_count": len(activities),
            **result
        }

        # =========================
        # IMPORTED HERE TO AVOID LOAD COST
        # =========================
        from report_builder import build_excel_report

        output = build_excel_report(response)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=carbon_report_v3.xlsx"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

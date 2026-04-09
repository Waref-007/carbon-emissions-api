from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import List, Annotated
from io import BytesIO
import io
import pandas as pd
import os

from openpyxl.drawing.image import Image as XLImage

from engine import run_emissions_engine, preprocess_uploaded_dataframe

app = FastAPI(title="Carbon Emissions API")

# Allow the client website to call this API
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


@app.get("/")
def root():
    return {"status": "ok", "message": "Carbon emissions API is running"}


@app.head("/")
def root_head():
    return


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/debug-routes")
def debug_routes():
    return {
        "routes": [
            "/",
            "/health",
            "/calculate",
            "/upload-calculate",
            "/upload-calculate-download"
        ]
    }


@app.post("/calculate")
def calculate(payload: dict):
    try:
        activities = payload.get("activities", [])

        if not isinstance(activities, list):
            raise HTTPException(status_code=400, detail="activities must be a list")

        result = run_emissions_engine({"activities": activities})
        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def normalize_uploaded_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        str(col).strip().lower().replace("\n", " ").replace("\r", " ")
        for col in df.columns
    ]
    return df


def read_uploaded_file(uploaded_file: UploadFile) -> pd.DataFrame:
    filename = (uploaded_file.filename or "").lower()
    content = uploaded_file.file.read()

    if filename.endswith(".csv"):
        df = pd.read_csv(BytesIO(content))
    elif filename.endswith(".xlsx") or filename.endswith(".xls"):
        df = pd.read_excel(BytesIO(content))
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file format for '{uploaded_file.filename}'. Please upload CSV or Excel files."
        )

    df = normalize_uploaded_columns(df)
    return df


def prepare_combined_dataframe(files: List[UploadFile]):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded.")

    dataframes = []
    uploaded_files_info = []

    for uploaded_file in files:
        df = read_uploaded_file(uploaded_file)

        if "source_file" not in df.columns:
            df["source_file"] = uploaded_file.filename

        dataframes.append(df)
        uploaded_files_info.append({
            "filename": uploaded_file.filename,
            "rows": len(df),
            "columns": list(df.columns)
        })

    combined_df = pd.concat(dataframes, ignore_index=True)

    required_columns = ["category", "unit", "amount"]
    missing_columns = [col for col in required_columns if col not in combined_df.columns]
    if missing_columns:
        raise HTTPException(
            status_code=400,
            detail={
                "message": f"Missing required columns: {missing_columns}",
                "detected_columns": list(combined_df.columns)
            }
        )

    combined_df_clean = preprocess_uploaded_dataframe(combined_df)
    return combined_df_clean, uploaded_files_info


@app.post("/upload-calculate")
async def upload_calculate(
    files: Annotated[
        List[UploadFile],
        File(description="Upload one or more CSV/XLSX activity datasets")
    ]
):
    try:
        combined_df_clean, uploaded_files_info = prepare_combined_dataframe(files)

        activities = combined_df_clean.to_dict(orient="records")
        result = run_emissions_engine({"activities": activities})

        result["uploaded_files"] = uploaded_files_info
        result["input_row_count"] = len(combined_df_clean)

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload-calculate-download")
async def upload_calculate_download(
    files: Annotated[
        List[UploadFile],
        File(description="Upload one or more CSV/XLSX activity datasets")
    ]
):
    try:
        combined_df_clean, uploaded_files_info = prepare_combined_dataframe(files)

        activities = combined_df_clean.to_dict(orient="records")
        result = run_emissions_engine({"activities": activities})

        # Convert outputs to DataFrames
        df_results = pd.DataFrame(result.get("line_items", []))
        df_errors = pd.DataFrame(result.get("errors", []))
        df_summary_scope = pd.DataFrame(result.get("summary_by_scope", []))
        df_summary_scope_cat = pd.DataFrame(result.get("summary_by_scope_category", []))

        # Totals sheet
        totals = result.get("totals", {})
        df_totals = pd.DataFrame([{
            "total_kgCO2e": totals.get("total_kgCO2e", 0),
            "total_tCO2e": totals.get("total_tCO2e", 0),
            "input_row_count": len(combined_df_clean),
            "uploaded_files_count": len(uploaded_files_info),
        }])

        df_uploaded_files = pd.DataFrame(uploaded_files_info)

        # Create Excel in memory
        output = io.BytesIO()

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_totals.to_excel(writer, sheet_name="Overview", index=False, startrow=6)
            df_summary_scope.to_excel(writer, sheet_name="Summary by Scope", index=False, startrow=6)
            df_summary_scope_cat.to_excel(writer, sheet_name="Summary by Category", index=False, startrow=6)
            df_results.to_excel(writer, sheet_name="Results", index=False, startrow=6)
            df_errors.to_excel(writer, sheet_name="Errors", index=False, startrow=6)
            df_uploaded_files.to_excel(writer, sheet_name="Uploaded Files", index=False, startrow=6)

            workbook = writer.book

            # Add titles to sheets
            for sheet_name in workbook.sheetnames:
                ws = workbook[sheet_name]
                ws["A1"] = "GS Carbon Emissions Report"
                ws["A2"] = "Generated from uploaded datasets"
                ws["A3"] = "Aligned to current configured emissions logic and reporting structure"

                # Add logo if present
                logo_path = "logo.png"
                if os.path.exists(logo_path):
                    try:
                        logo = XLImage(logo_path)
                        logo.width = 140
                        logo.height = 60
                        ws.add_image(logo, "E1")
                    except Exception:
                        pass

        output.seek(0)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=carbon_emissions_report.xlsx"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

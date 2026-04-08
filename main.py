from fastapi import FastAPI, UploadFile, File, HTTPException
from typing import List, Annotated
from io import BytesIO
import pandas as pd

from engine import run_emissions_engine, preprocess_uploaded_dataframe

app = FastAPI(title="Carbon Emissions API")


@app.get("/")
def root():
    return {"status": "ok", "message": "Carbon emissions API is running"}


@app.get("/health")
def health():
    return {"status": "healthy"}


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


def read_uploaded_file(uploaded_file: UploadFile) -> pd.DataFrame:
    filename = (uploaded_file.filename or "").lower()
    content = uploaded_file.file.read()

    if filename.endswith(".csv"):
        return pd.read_csv(BytesIO(content))
    elif filename.endswith(".xlsx") or filename.endswith(".xls"):
        return pd.read_excel(BytesIO(content))
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file format for '{uploaded_file.filename}'. Please upload CSV or Excel files."
        )


@app.post("/upload-calculate")
async def upload_calculate(
    files: Annotated[
        List[UploadFile],
        File(description="Upload one or more CSV/XLSX activity datasets")
    ]
):
    try:
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
                "rows": len(df)
            })

        combined_df = pd.concat(dataframes, ignore_index=True)

        required_columns = ["category", "unit", "amount"]
        missing_columns = [col for col in required_columns if col not in combined_df.columns]
        if missing_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {missing_columns}"
            )

        combined_df_clean = preprocess_uploaded_dataframe(combined_df)

        activities = combined_df_clean.to_dict(orient="records")
        result = run_emissions_engine({"activities": activities})

        result["uploaded_files"] = uploaded_files_info
        result["input_row_count"] = len(combined_df_clean)

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

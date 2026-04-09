from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import List, Annotated
from io import BytesIO
import io
import pandas as pd
import os

from openpyxl.drawing.image import Image as XLImage
from openpyxl.chart import BarChart, Reference

from engine import run_emissions_engine, preprocess_uploaded_dataframe

app = FastAPI(title="Carbon Emissions API")

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


def build_analytics(result: dict) -> dict:
    df_results = pd.DataFrame(result.get("line_items", []))

    analytics = {}

    if df_results.empty:
        analytics["average_emissions_per_row_kgco2e"] = 0
        return analytics

    analytics["average_emissions_per_row_kgco2e"] = round(
        float(df_results["emissions_kgCO2e"].mean()), 4
    )

    if "category" in df_results.columns:
        df_cat = (
            df_results.groupby("category", as_index=False)["emissions_kgCO2e"]
            .sum()
            .sort_values("emissions_kgCO2e", ascending=False)
        )
        if not df_cat.empty:
            analytics["top_category_by_kgco2e"] = {
                "category": df_cat.iloc[0]["category"],
                "emissions_kgCO2e": round(float(df_cat.iloc[0]["emissions_kgCO2e"]), 4)
            }

    if "site_name" in df_results.columns and df_results["site_name"].notna().any():
        df_site = (
            df_results.dropna(subset=["site_name"])
            .groupby("site_name", as_index=False)["emissions_kgCO2e"]
            .sum()
            .sort_values("emissions_kgCO2e", ascending=False)
        )
        if not df_site.empty:
            analytics["top_site_by_kgco2e"] = {
                "site_name": df_site.iloc[0]["site_name"],
                "emissions_kgCO2e": round(float(df_site.iloc[0]["emissions_kgCO2e"]), 4)
            }

    return analytics


def add_excel_chart(ws, title, data_col, category_col, start_row, end_row, anchor):
    chart = BarChart()
    chart.title = title
    chart.y_axis.title = "kg CO2e"
    chart.x_axis.title = "Category"

    data = Reference(ws, min_col=data_col, min_row=start_row, max_row=end_row)
    categories = Reference(ws, min_col=category_col, min_row=start_row + 1, max_row=end_row)

    chart.add_data(data, titles_from_data=True)
    chart.set_categories(categories)
    chart.height = 8
    chart.width = 16
    ws.add_chart(chart, anchor)


@app.post("/upload-calculate")
async def upload_calculate(
    files: Annotated[List[UploadFile], File(description="Upload one or more CSV/XLSX activity datasets")],
    privacy_consent: Annotated[str, Form(...)]
):
    try:
        if str(privacy_consent).lower() != "true":
            raise HTTPException(status_code=400, detail="Privacy consent is required.")

        combined_df_clean, uploaded_files_info = prepare_combined_dataframe(files)

        activities = combined_df_clean.to_dict(orient="records")
        result = run_emissions_engine({"activities": activities})

        result["uploaded_files"] = uploaded_files_info
        result["input_row_count"] = len(combined_df_clean)
        result["privacy_consent"] = True
        result["analytics"] = build_analytics(result)

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload-calculate-download")
async def upload_calculate_download(
    files: Annotated[List[UploadFile], File(description="Upload one or more CSV/XLSX activity datasets to calculate emissions and downlaod report")],
    privacy_consent: Annotated[str, Form(...)]
):
    try:
        if str(privacy_consent).lower() != "true":
            raise HTTPException(status_code=400, detail="Privacy consent is required.")

        combined_df_clean, uploaded_files_info = prepare_combined_dataframe(files)

        activities = combined_df_clean.to_dict(orient="records")
        result = run_emissions_engine({"activities": activities})

        result["uploaded_files"] = uploaded_files_info
        result["input_row_count"] = len(combined_df_clean)
        result["privacy_consent"] = True
        result["analytics"] = build_analytics(result)

        df_results = pd.DataFrame(result.get("line_items", []))
        df_errors = pd.DataFrame(result.get("errors", []))
        df_summary_scope = pd.DataFrame(result.get("summary_by_scope", []))
        df_summary_scope_cat = pd.DataFrame(result.get("summary_by_scope_category", []))
        df_uploaded_files = pd.DataFrame(uploaded_files_info)
        df_analytics = pd.DataFrame(
            [{"metric": k, "value": str(v)} for k, v in result.get("analytics", {}).items()]
        )

        totals = result.get("totals", {})
        df_overview = pd.DataFrame([{
            "total_kgCO2e": totals.get("total_kgCO2e", 0),
            "total_tCO2e": totals.get("total_tCO2e", 0),
            "input_row_count": len(combined_df_clean),
            "uploaded_files_count": len(uploaded_files_info),
            "privacy_consent": True
        }])

        output = io.BytesIO()

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_overview.to_excel(writer, sheet_name="Overview", index=False, startrow=6)
            df_summary_scope.to_excel(writer, sheet_name="Summary by Scope", index=False, startrow=6)
            df_summary_scope_cat.to_excel(writer, sheet_name="Summary by Category", index=False, startrow=6)
            df_results.to_excel(writer, sheet_name="Results", index=False, startrow=6)
            df_errors.to_excel(writer, sheet_name="Errors", index=False, startrow=6)
            df_uploaded_files.to_excel(writer, sheet_name="Uploaded Files", index=False, startrow=6)
            df_analytics.to_excel(writer, sheet_name="Analytics", index=False, startrow=6)

            workbook = writer.book

            for sheet_name in workbook.sheetnames:
                ws = workbook[sheet_name]
                ws["A1"] = "GS Carbon Emissions Report"
                ws["A2"] = "Generated from uploaded datasets"
                ws["A3"] = "Aligned with GHG Protocol and DEFRA 2025"

                logo_path = "logo.png"
                if os.path.exists(logo_path):
                    try:
                        logo = XLImage(logo_path)
                        logo.width = 140
                        logo.height = 60
                        ws.add_image(logo, "E1")
                    except Exception:
                        pass

            # Create chart sheet
            chart_ws = workbook.create_sheet("Charts")
            chart_ws["A1"] = "GS Carbon Emissions Report"
            chart_ws["A2"] = "Generated from uploaded datasets"
            chart_ws["A3"] = "°Aligned to current configured emissions logic and reporting structure °Aligned with GHG Protocol and DEFRA 2025"

            logo_path = "logo.png"
            if os.path.exists(logo_path):
                try:
                    logo = XLImage(logo_path)
                    logo.width = 140
                    logo.height = 60
                    chart_ws.add_image(logo, "L1")
                except Exception:
                    pass

            # Scope chart data
            chart_ws["A6"] = "Scope"
            chart_ws["B6"] = "kg CO2e"
            for i, row in enumerate(result.get("summary_by_scope", []), start=7):
                chart_ws[f"A{i}"] = row.get("scope")
                chart_ws[f"B{i}"] = row.get("emissions_kgCO2e")

            # Category chart data
            chart_ws["D6"] = "Category"
            chart_ws["E6"] = "kg CO2e"
            for i, row in enumerate(result.get("summary_by_scope_category", []), start=7):
                chart_ws[f"D{i}"] = f"{row.get('scope')} - {row.get('category')}"
                chart_ws[f"E{i}"] = row.get("emissions_kgCO2e")

            if len(result.get("summary_by_scope", [])) > 0:
                add_excel_chart(chart_ws, "Emissions by Scope", 2, 1, 6, 6 + len(result.get("summary_by_scope", [])), "G6")

            if len(result.get("summary_by_scope_category", [])) > 0:
                add_excel_chart(chart_ws, "Emissions by Scope and Category", 5, 4, 6, 6 + len(result.get("summary_by_scope_category", [])), "G24")

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

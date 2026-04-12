from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import List, Annotated
from io import BytesIO
import io
import pandas as pd
import os
import requests

from openpyxl.drawing.image import Image as XLImage
from openpyxl.chart import BarChart, PieChart, Reference

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
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")


def normalize_uploaded_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        str(col).strip().lower().replace("\n", " ").replace("\r", " ")
        for col in df.columns
    ]
    return df


def safe_string(value):
    if value is None:
        return ""
    return str(value).strip()


def read_uploaded_file(uploaded_file: UploadFile):
    filename = (uploaded_file.filename or "").lower()
    content = uploaded_file.file.read()
    file_warnings = []

    try:
        if filename.endswith(".csv"):
            try:
                df = pd.read_csv(
                    BytesIO(content),
                    dtype=str,
                    keep_default_na=False,
                    engine="python",
                    on_bad_lines="skip"
                )
                file_warnings.append("CSV parsed in tolerant mode; malformed lines may have been skipped.")
            except Exception as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Could not read CSV file '{uploaded_file.filename}': {type(e).__name__}: {str(e)}"
                )

        elif filename.endswith(".xlsx") or filename.endswith(".xls"):
            try:
                df = pd.read_excel(
                    BytesIO(content),
                    dtype=str
                )
            except Exception as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Could not read Excel file '{uploaded_file.filename}': {type(e).__name__}: {str(e)}"
                )
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file format for '{uploaded_file.filename}'. Please upload CSV or Excel files."
            )

        df = normalize_uploaded_columns(df)

        if df.empty:
            file_warnings.append("File was readable but contained no data rows.")

        return df, file_warnings

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected file read error for '{uploaded_file.filename}': {type(e).__name__}: {str(e)}"
        )


def prepare_combined_dataframe(files: List[UploadFile]):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded.")

    dataframes = []
    uploaded_files_info = []
    file_level_errors = []

    required_columns = ["category", "unit", "amount"]

    for uploaded_file in files:
        try:
            df, file_warnings = read_uploaded_file(uploaded_file)

            if "source_file" not in df.columns:
                df["source_file"] = uploaded_file.filename

            if "row_index_original" not in df.columns:
                df["row_index_original"] = range(1, len(df) + 1)

            missing_in_file = [col for col in required_columns if col not in df.columns]
            for col in missing_in_file:
                df[col] = ""

            if missing_in_file:
                file_level_errors.append({
                    "source_file": uploaded_file.filename,
                    "error": f"Missing required columns added as blank: {missing_in_file}"
                })

            for warning in file_warnings:
                file_level_errors.append({
                    "source_file": uploaded_file.filename,
                    "error": warning
                })

            dataframes.append(df)
            uploaded_files_info.append({
                "filename": uploaded_file.filename,
                "rows": len(df),
                "columns": list(df.columns),
                "missing_required_columns": missing_in_file
            })

        except HTTPException as e:
            file_level_errors.append({
                "source_file": uploaded_file.filename,
                "error": f"{e.detail}"
            })
        except Exception as e:
            file_level_errors.append({
                "source_file": uploaded_file.filename,
                "error": f"Failed to process file: {type(e).__name__}: {str(e)}"
            })

    if not dataframes:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "None of the uploaded files could be processed.",
                "file_level_errors": file_level_errors
            }
        )

    combined_df = pd.concat(dataframes, ignore_index=True)

    try:
        combined_df_clean = preprocess_uploaded_dataframe(combined_df)
    except Exception as e:
        file_level_errors.append({
            "source_file": "preprocess_uploaded_dataframe",
            "error": f"Preprocessing failed, using fallback normalization only: {type(e).__name__}: {str(e)}"
        })
        combined_df_clean = combined_df.copy()

    for col in required_columns:
        if col not in combined_df_clean.columns:
            combined_df_clean[col] = ""

    if "source_file" not in combined_df_clean.columns:
        combined_df_clean["source_file"] = ""

    if "row_index_original" not in combined_df_clean.columns:
        combined_df_clean["row_index_original"] = range(1, len(combined_df_clean) + 1)

    combined_df_clean = combined_df_clean.fillna("")

    return combined_df_clean, uploaded_files_info, file_level_errors


def safe_run_emissions_engine(activities: list):
    """
    Try running the engine on all activities.
    If the engine crashes on messy data, fall back to row-by-row processing
    so that a partial report can still be generated.
    """
    try:
        result = run_emissions_engine({"activities": activities})
        if not isinstance(result, dict):
            raise ValueError("Engine did not return a dictionary.")
        return result
    except Exception as e:
        fallback_errors = [{
            "source_file": "engine",
            "error": f"Bulk engine processing failed; falling back to row-by-row processing: {type(e).__name__}: {str(e)}"
        }]

        successful_line_items = []
        collected_errors = []

        for idx, activity in enumerate(activities):
            try:
                row_result = run_emissions_engine({"activities": [activity]})

                row_line_items = row_result.get("line_items", [])
                row_errors = row_result.get("errors", [])

                if row_line_items:
                    successful_line_items.extend(row_line_items)

                if row_errors:
                    collected_errors.extend(row_errors)

            except Exception as row_error:
                collected_errors.append({
                    "row_index": activity.get("row_index_original", idx + 1),
                    "source_file": activity.get("source_file", ""),
                    "error": f"Row processing failed: {type(row_error).__name__}: {str(row_error)}"
                })

        result = build_result_from_line_items(successful_line_items, collected_errors + fallback_errors)
        return result


def build_result_from_line_items(line_items: list, errors: list):
    df_results = pd.DataFrame(line_items) if line_items else pd.DataFrame()

    total_kg = 0.0
    total_t = 0.0
    summary_by_scope = []
    summary_by_scope_category = []

    if not df_results.empty and "emissions_kgCO2e" in df_results.columns:
        df_results["emissions_kgCO2e"] = pd.to_numeric(df_results["emissions_kgCO2e"], errors="coerce").fillna(0)

        total_kg = round(float(df_results["emissions_kgCO2e"].sum()), 4)
        total_t = round(total_kg / 1000, 4)

        if "scope" in df_results.columns:
            df_scope = (
                df_results.groupby("scope", as_index=False)["emissions_kgCO2e"]
                .sum()
                .sort_values("emissions_kgCO2e", ascending=False)
            )
            if not df_scope.empty:
                df_scope["emissions_tCO2e"] = (df_scope["emissions_kgCO2e"] / 1000).round(4)
                df_scope["emissions_kgCO2e"] = df_scope["emissions_kgCO2e"].round(4)
                summary_by_scope = df_scope.to_dict(orient="records")

        if "scope" in df_results.columns and "category" in df_results.columns:
            df_scope_cat = (
                df_results.groupby(["scope", "category"], as_index=False)["emissions_kgCO2e"]
                .sum()
                .sort_values("emissions_kgCO2e", ascending=False)
            )
            if not df_scope_cat.empty:
                df_scope_cat["emissions_tCO2e"] = (df_scope_cat["emissions_kgCO2e"] / 1000).round(4)
                df_scope_cat["emissions_kgCO2e"] = df_scope_cat["emissions_kgCO2e"].round(4)
                summary_by_scope_category = df_scope_cat.to_dict(orient="records")

    return {
        "totals": {
            "total_kgCO2e": total_kg,
            "total_tCO2e": total_t
        },
        "summary_by_scope": summary_by_scope,
        "summary_by_scope_category": summary_by_scope_category,
        "line_items": line_items,
        "errors": errors
    }


def build_analytics(result: dict) -> dict:
    df_results = pd.DataFrame(result.get("line_items", []))
    analytics = {}

    if df_results.empty:
        analytics["average_emissions_per_row_kgco2e"] = 0
        return analytics

    if "emissions_kgCO2e" in df_results.columns:
        df_results["emissions_kgCO2e"] = pd.to_numeric(df_results["emissions_kgCO2e"], errors="coerce").fillna(0)
        analytics["average_emissions_per_row_kgco2e"] = round(
            float(df_results["emissions_kgCO2e"].mean()), 4
        )
    else:
        analytics["average_emissions_per_row_kgco2e"] = 0

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
            df_results[df_results["site_name"].astype(str).str.strip() != ""]
            .groupby("site_name", as_index=False)["emissions_kgCO2e"]
            .sum()
            .sort_values("emissions_kgCO2e", ascending=False)
        )
        if not df_site.empty:
            analytics["top_site_by_kgco2e"] = {
                "site_name": df_site.iloc[0]["site_name"],
                "emissions_kgCO2e": round(float(df_site.iloc[0]["emissions_kgCO2e"]), 4)
            }

    if "reporting_period" in df_results.columns and df_results["reporting_period"].notna().any():
        df_period = (
            df_results[df_results["reporting_period"].astype(str).str.strip() != ""]
            .groupby("reporting_period", as_index=False)["emissions_kgCO2e"]
            .sum()
            .sort_values("reporting_period")
        )
        analytics["period_trend"] = df_period.to_dict(orient="records")

    return analytics


def build_error_summary(errors: list) -> list:
    if not errors:
        return []

    df_errors = pd.DataFrame(errors)
    if "error" not in df_errors.columns:
        return []

    grouped = df_errors["error"].astype(str).value_counts().reset_index()
    grouped.columns = ["error", "count"]

    return grouped.to_dict(orient="records")


def add_bar_chart(ws, title, data_col, category_col, start_row, end_row, anchor):
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


def add_pie_chart(ws, title, data_col, category_col, start_row, end_row, anchor):
    chart = PieChart()
    chart.title = title

    data = Reference(ws, min_col=data_col, min_row=start_row, max_row=end_row)
    labels = Reference(ws, min_col=category_col, min_row=start_row + 1, max_row=end_row)

    chart.add_data(data, titles_from_data=True)
    chart.set_categories(labels)
    chart.height = 8
    chart.width = 12
    ws.add_chart(chart, anchor)


def send_lead_to_logger(user_data: dict, result: dict, uploaded_files_info: list):
    url = os.getenv("LEAD_LOGGER_URL")
    if not url:
        return

    totals = result.get("totals", {})
    analytics = result.get("analytics", {})

    payload = {
        "full_name": user_data.get("full_name", ""),
        "email": user_data.get("email", ""),
        "company_name": user_data.get("company_name", ""),
        "phone_number": user_data.get("phone_number", ""),
        "privacy_consent": user_data.get("privacy_consent", False),
        "contact_consent": user_data.get("contact_consent", False),
        "uploaded_files_count": len(uploaded_files_info),
        "input_row_count": result.get("input_row_count", 0),
        "total_kgco2e": totals.get("total_kgCO2e", 0),
        "total_tco2e": totals.get("total_tCO2e", 0),
        "top_category": (analytics.get("top_category_by_kgco2e") or {}).get("category", ""),
        "top_site": (analytics.get("top_site_by_kgco2e") or {}).get("site_name", ""),
        "source": "website_calculator"
    }

    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Lead logger failed: {type(e).__name__}: {str(e)}")


@app.post("/upload-calculate")
async def upload_calculate(
    files: Annotated[List[UploadFile], File(description="Upload one or more CSV/XLSX activity datasets")],
    privacy_consent: Annotated[str, Form(...)],
    contact_consent: Annotated[str, Form(...)],
    full_name: Annotated[str, Form(...)],
    email: Annotated[str, Form(...)],
    company_name: Annotated[str, Form(...)],
    phone_number: Annotated[str, Form(...)]
):
    try:
        if str(privacy_consent).lower() != "true":
            raise HTTPException(status_code=400, detail="Privacy consent is required.")

        if str(contact_consent).lower() != "true":
            raise HTTPException(status_code=400, detail="Contact consent is required.")

        if not safe_string(full_name):
            raise HTTPException(status_code=400, detail="Full name is required.")

        if not safe_string(email):
            raise HTTPException(status_code=400, detail="Email is required.")

        if not safe_string(company_name):
            raise HTTPException(status_code=400, detail="Company name is required.")

        if not safe_string(phone_number):
            raise HTTPException(status_code=400, detail="Phone number is required.")

        combined_df_clean, uploaded_files_info, file_level_errors = prepare_combined_dataframe(files)

        activities = combined_df_clean.to_dict(orient="records")
        full_result = safe_run_emissions_engine(activities)

        analytics = build_analytics(full_result)

        row_errors = full_result.get("errors", [])
        all_errors = file_level_errors + row_errors
        error_summary = build_error_summary(all_errors)

        result = {
            "totals": full_result.get("totals", {}),
            "summary_by_scope": full_result.get("summary_by_scope", []),
            "summary_by_scope_category": full_result.get("summary_by_scope_category", []),
            "uploaded_files": uploaded_files_info,
            "input_row_count": len(combined_df_clean),
            "privacy_consent": True,
            "contact_consent": True,
            "user": {
                "full_name": full_name,
                "email": email,
                "company_name": company_name,
                "phone_number": phone_number
            },
            "analytics": analytics,
            "error_count": len(all_errors),
            "errors_preview": all_errors[:50],
            "error_summary": error_summary
        }

        send_lead_to_logger(
            {
                "full_name": full_name,
                "email": email,
                "company_name": company_name,
                "phone_number": phone_number,
                "privacy_consent": True,
                "contact_consent": True
            },
            {
                "totals": full_result.get("totals", {}),
                "analytics": analytics,
                "input_row_count": len(combined_df_clean)
            },
            uploaded_files_info
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")


@app.post("/upload-calculate-download")
async def upload_calculate_download(
    files: Annotated[List[UploadFile], File(description="Upload one or more CSV/XLSX activity datasets")],
    privacy_consent: Annotated[str, Form(...)],
    contact_consent: Annotated[str, Form(...)],
    full_name: Annotated[str, Form(...)],
    email: Annotated[str, Form(...)],
    company_name: Annotated[str, Form(...)],
    phone_number: Annotated[str, Form(...)]
):
    try:
        if str(privacy_consent).lower() != "true":
            raise HTTPException(status_code=400, detail="Privacy consent is required.")

        if str(contact_consent).lower() != "true":
            raise HTTPException(status_code=400, detail="Contact consent is required.")

        if not safe_string(full_name):
            raise HTTPException(status_code=400, detail="Full name is required.")

        if not safe_string(email):
            raise HTTPException(status_code=400, detail="Email is required.")

        if not safe_string(company_name):
            raise HTTPException(status_code=400, detail="Company name is required.")

        if not safe_string(phone_number):
            raise HTTPException(status_code=400, detail="Phone number is required.")

        combined_df_clean, uploaded_files_info, file_level_errors = prepare_combined_dataframe(files)

        activities = combined_df_clean.to_dict(orient="records")
        result = safe_run_emissions_engine(activities)

        result["errors"] = file_level_errors + result.get("errors", [])
        result["uploaded_files"] = uploaded_files_info
        result["input_row_count"] = len(combined_df_clean)
        result["privacy_consent"] = True
        result["contact_consent"] = True
        result["user"] = {
            "full_name": full_name,
            "email": email,
            "company_name": company_name,
            "phone_number": phone_number
        }
        result["analytics"] = build_analytics(result)

        send_lead_to_logger(
            {
                "full_name": full_name,
                "email": email,
                "company_name": company_name,
                "phone_number": phone_number,
                "privacy_consent": True,
                "contact_consent": True
            },
            result,
            uploaded_files_info
        )

        df_results = pd.DataFrame(result.get("line_items", []))
        df_errors = pd.DataFrame(result.get("errors", []))
        df_summary_scope = pd.DataFrame(result.get("summary_by_scope", []))
        df_summary_scope_cat = pd.DataFrame(result.get("summary_by_scope_category", []))
        df_uploaded_files = pd.DataFrame(uploaded_files_info)
        df_analytics = pd.DataFrame(
            [{"metric": k, "value": str(v)} for k, v in result.get("analytics", {}).items()]
        )
        df_user = pd.DataFrame([result["user"]])
        df_error_summary = pd.DataFrame(build_error_summary(result.get("errors", [])))

        totals = result.get("totals", {})
        df_overview = pd.DataFrame([{
            "total_kgCO2e": totals.get("total_kgCO2e", 0),
            "total_tCO2e": totals.get("total_tCO2e", 0),
            "input_row_count": len(combined_df_clean),
            "uploaded_files_count": len(uploaded_files_info),
            "privacy_consent": True,
            "contact_consent": True
        }])

        output = io.BytesIO()

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_overview.to_excel(writer, sheet_name="Overview", index=False, startrow=6)
            df_summary_scope.to_excel(writer, sheet_name="Summary by Scope", index=False, startrow=6)
            df_summary_scope_cat.to_excel(writer, sheet_name="Summary by Category", index=False, startrow=6)
            df_results.to_excel(writer, sheet_name="Results", index=False, startrow=6)
            df_errors.to_excel(writer, sheet_name="Errors", index=False, startrow=6)
            df_error_summary.to_excel(writer, sheet_name="Error Summary", index=False, startrow=6)
            df_uploaded_files.to_excel(writer, sheet_name="Uploaded Files", index=False, startrow=6)
            df_analytics.to_excel(writer, sheet_name="Analytics", index=False, startrow=6)
            df_user.to_excel(writer, sheet_name="User Details", index=False, startrow=6)

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
                        logo.height = 70
                        ws.add_image(logo, "L1")
                    except Exception:
                        pass

            chart_ws = workbook.create_sheet("Charts")
            chart_ws["A1"] = "GS Carbon Emissions Report"
            chart_ws["A2"] = "Generated from uploaded datasets"
            chart_ws["A3"] = "Aligned with GHG Protocol and DEFRA 2025"

            logo_path = "logo.png"
            if os.path.exists(logo_path):
                try:
                    logo = XLImage(logo_path)
                    logo.width = 140
                    logo.height = 70
                    chart_ws.add_image(logo, "L1")
                except Exception:
                    pass

            chart_ws["A6"] = "Scope"
            chart_ws["B6"] = "kg CO2e"
            for i, row in enumerate(result.get("summary_by_scope", []), start=7):
                chart_ws[f"A{i}"] = row.get("scope")
                chart_ws[f"B{i}"] = row.get("emissions_kgCO2e")

            chart_ws["D6"] = "Category"
            chart_ws["E6"] = "kg CO2e"
            for i, row in enumerate(result.get("summary_by_scope_category", []), start=7):
                chart_ws[f"D{i}"] = f"{row.get('scope')} - {row.get('category')}"
                chart_ws[f"E{i}"] = row.get("emissions_kgCO2e")

            chart_ws["J6"] = "Scope"
            chart_ws["K6"] = "kg CO2e"
            for i, row in enumerate(result.get("summary_by_scope", []), start=7):
                chart_ws[f"J{i}"] = row.get("scope")
                chart_ws[f"K{i}"] = row.get("emissions_kgCO2e")

            chart_ws["N6"] = "Category"
            chart_ws["O6"] = "kg CO2e"
            for i, row in enumerate(result.get("summary_by_scope_category", []), start=7):
                chart_ws[f"N{i}"] = f"{row.get('scope')} - {row.get('category')}"
                chart_ws[f"O{i}"] = row.get("emissions_kgCO2e")

            if len(result.get("summary_by_scope", [])) > 0:
                add_bar_chart(
                    chart_ws,
                    "Emissions by Scope",
                    2,
                    1,
                    6,
                    6 + len(result.get("summary_by_scope", [])),
                    "G6"
                )

                add_pie_chart(
                    chart_ws,
                    "Scope Share",
                    11,
                    10,
                    6,
                    6 + len(result.get("summary_by_scope", [])),
                    "Q6"
                )

            if len(result.get("summary_by_scope_category", [])) > 0:
                add_bar_chart(
                    chart_ws,
                    "Emissions by Scope and Category",
                    5,
                    4,
                    6,
                    6 + len(result.get("summary_by_scope_category", [])),
                    "G24"
                )

                add_pie_chart(
                    chart_ws,
                    "Category Share",
                    15,
                    14,
                    6,
                    6 + len(result.get("summary_by_scope_category", [])),
                    "Q24"
                )

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
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")

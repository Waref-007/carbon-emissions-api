from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import List, Annotated
from io import BytesIO
import io
import os
import re
import requests
import pandas as pd

from openpyxl.drawing.image import Image as XLImage
from openpyxl.chart import BarChart, PieChart, Reference
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

from engine import run_emissions_engine, preprocess_uploaded_dataframe

app = FastAPI(title="Carbon Emissions API")


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
# BASIC ROUTES
# =========================
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
            "/upload-calculate-download",
        ]
    }


# =========================
# GENERAL HELPERS
# =========================
def safe_string(value):
    if value is None:
        return ""
    return str(value).strip()


def to_bool_string_true(value: str) -> bool:
    return str(value).strip().lower() == "true"


def normalize_uploaded_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        str(col).strip().lower().replace("\n", " ").replace("\r", " ")
        for col in df.columns
    ]
    return df


def sanitize_sheet_name(name: str) -> str:
    if not name:
        return "Sheet"
    cleaned = re.sub(r"[\\/*?:\[\]]", "", str(name)).strip()
    return cleaned[:31] if cleaned else "Sheet"


def auto_fit_columns(ws, min_width=12, max_width=45):
    for col_cells in ws.columns:
        length = 0
        col_letter = get_column_letter(col_cells[0].column)
        for cell in col_cells:
            try:
                value = "" if cell.value is None else str(cell.value)
                length = max(length, len(value))
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(max(length + 2, min_width), max_width)


def style_report_sheet(ws, title="GS Carbon Emissions Report", subtitle="Generated from uploaded datasets"):
    ws["A1"] = title
    ws["A2"] = subtitle
    ws["A3"] = "Aligned with GHG Protocol and DEFRA 2025"

    ws["A1"].font = Font(size=16, bold=True)
    ws["A2"].font = Font(size=11, italic=True)
    ws["A3"].font = Font(size=10)

    header_fill = PatternFill(fill_type="solid", start_color="D9EAD3", end_color="D9EAD3")

    for cell in ws[7]:
        cell.font = Font(bold=True)
        cell.fill = header_fill

    logo_path = "logo.png"
    if os.path.exists(logo_path):
        try:
            logo = XLImage(logo_path)
            logo.width = 140
            logo.height = 70
            ws.add_image(logo, "L1")
        except Exception:
            pass


def add_bar_chart(ws, title, data_col, category_col, start_row, end_row, anchor):
    if end_row <= start_row:
        return

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
    if end_row <= start_row:
        return

    chart = PieChart()
    chart.title = title

    data = Reference(ws, min_col=data_col, min_row=start_row, max_row=end_row)
    labels = Reference(ws, min_col=category_col, min_row=start_row + 1, max_row=end_row)

    chart.add_data(data, titles_from_data=True)
    chart.set_categories(labels)
    chart.height = 8
    chart.width = 12
    ws.add_chart(chart, anchor)


def build_error_summary(errors: list) -> list:
    if not errors:
        return []

    df_errors = pd.DataFrame(errors)
    if "error" not in df_errors.columns:
        return []

    grouped = df_errors["error"].astype(str).value_counts().reset_index()
    grouped.columns = ["error", "count"]
    return grouped.to_dict(orient="records")


def build_analytics_from_line_items(result: dict) -> dict:
    """
    Fallback analytics builder.
    Used only when the engine did not already return analytics.
    """
    existing = result.get("analytics")
    if isinstance(existing, dict) and existing:
        return existing

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
            analytics["top_categories_by_kgco2e"] = df_cat.head(5).to_dict(orient="records")

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
            analytics["top_sites_by_kgco2e"] = df_site.head(5).to_dict(orient="records")

    if "reporting_period" in df_results.columns and df_results["reporting_period"].notna().any():
        df_period = (
            df_results[df_results["reporting_period"].astype(str).str.strip() != ""]
            .groupby("reporting_period", as_index=False)["emissions_kgCO2e"]
            .sum()
            .sort_values("reporting_period")
        )
        analytics["period_trend"] = df_period.to_dict(orient="records")

    return analytics


def validate_user_inputs(
    privacy_consent: str,
    contact_consent: str,
    full_name: str,
    email: str,
    company_name: str,
    phone_number: str,
):
    if not to_bool_string_true(privacy_consent):
        raise HTTPException(status_code=400, detail="Privacy consent is required.")

    if not to_bool_string_true(contact_consent):
        raise HTTPException(status_code=400, detail="Contact consent is required.")

    if not safe_string(full_name):
        raise HTTPException(status_code=400, detail="Full name is required.")

    if not safe_string(email):
        raise HTTPException(status_code=400, detail="Email is required.")

    if not safe_string(company_name):
        raise HTTPException(status_code=400, detail="Company name is required.")

    if not safe_string(phone_number):
        raise HTTPException(status_code=400, detail="Phone number is required.")


def build_user_object(full_name, email, company_name, phone_number):
    return {
        "full_name": full_name,
        "email": email,
        "company_name": company_name,
        "phone_number": phone_number
    }


# =========================
# FILE READING
# =========================
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
                df = pd.read_excel(BytesIO(content), dtype=str)
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


# =========================
# ENGINE SAFE WRAPPER
# =========================
def build_result_from_line_items(
    line_items: list,
    errors: list,
    data_quality: dict | None = None,
    unmapped_categories: list | None = None,
    factor_transparency: list | None = None
):
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
                total_scope_kg = float(df_scope["emissions_kgCO2e"].sum()) if not df_scope.empty else 0.0
                df_scope["percent_of_total"] = df_scope["emissions_kgCO2e"].apply(
                    lambda x: round((float(x) / total_scope_kg) * 100, 2) if total_scope_kg > 0 else 0.0
                )
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
        "errors": errors,
        "data_quality": data_quality or {},
        "unmapped_categories": unmapped_categories or [],
        "factor_transparency": factor_transparency or [],
        "analytics": build_analytics_from_line_items({"line_items": line_items})
    }


def safe_run_emissions_engine(activities: list):
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
        all_factor_rows = []
        all_unmapped_categories = []
        best_data_quality = {
            "total_rows": len(activities),
            "successful_rows": 0,
            "errored_rows": 0,
            "coverage_percent": 0,
            "mapped_category_rows": 0,
            "inferred_category_rows": 0,
            "exact_factor_rows": 0,
            "estimated_rows": 0,
            "official_factor_rows": 0,
            "starter_factor_rows": 0,
            "valid_rows_percent": 0,
            "invalid_rows_percent": 0
        }

        for idx, activity in enumerate(activities):
            try:
                row_result = run_emissions_engine({"activities": [activity]})

                row_line_items = row_result.get("line_items", [])
                row_errors = row_result.get("errors", [])
                row_factors = row_result.get("factor_transparency", [])
                row_unmapped = row_result.get("unmapped_categories", [])
                row_quality = row_result.get("data_quality", {})

                if row_line_items:
                    successful_line_items.extend(row_line_items)

                if row_errors:
                    collected_errors.extend(row_errors)

                if row_factors:
                    all_factor_rows.extend(row_factors)

                if row_unmapped:
                    all_unmapped_categories.extend(row_unmapped)

                if row_quality:
                    best_data_quality["successful_rows"] += int(row_quality.get("successful_rows", 0))
                    best_data_quality["errored_rows"] += int(row_quality.get("errored_rows", 0))
                    best_data_quality["mapped_category_rows"] += int(row_quality.get("mapped_category_rows", 0))
                    best_data_quality["inferred_category_rows"] += int(row_quality.get("inferred_category_rows", 0))
                    best_data_quality["exact_factor_rows"] += int(row_quality.get("exact_factor_rows", 0))
                    best_data_quality["estimated_rows"] += int(row_quality.get("estimated_rows", 0))
                    best_data_quality["official_factor_rows"] += int(row_quality.get("official_factor_rows", 0))
                    best_data_quality["starter_factor_rows"] += int(row_quality.get("starter_factor_rows", 0))

            except Exception as row_error:
                collected_errors.append({
                    "row_index": activity.get("row_index_original", idx + 1),
                    "source_file": activity.get("source_file", ""),
                    "error": f"Row processing failed: {type(row_error).__name__}: {str(row_error)}"
                })

        if best_data_quality["total_rows"] > 0:
            best_data_quality["coverage_percent"] = round(
                (best_data_quality["successful_rows"] / best_data_quality["total_rows"]) * 100, 2
            )
            best_data_quality["valid_rows_percent"] = round(
                (best_data_quality["successful_rows"] / best_data_quality["total_rows"]) * 100, 2
            )
            best_data_quality["invalid_rows_percent"] = round(
                (best_data_quality["errored_rows"] / best_data_quality["total_rows"]) * 100, 2
            )

        result = build_result_from_line_items(
            line_items=successful_line_items,
            errors=collected_errors + fallback_errors,
            data_quality=best_data_quality,
            unmapped_categories=all_unmapped_categories,
            factor_transparency=all_factor_rows
        )
        return result


# =========================
# RESULT ENRICHMENT
# =========================
def merge_file_errors_with_result(base_result: dict, file_level_errors: list) -> dict:
    result = dict(base_result)
    combined_errors = file_level_errors + result.get("errors", [])
    result["errors"] = combined_errors

    if "error_summary" not in result or not result.get("error_summary"):
        result["error_summary"] = build_error_summary(combined_errors)

    if "errors_preview" not in result or not result.get("errors_preview"):
        result["errors_preview"] = combined_errors[:50]

    return result


def build_api_response(
    full_result: dict,
    uploaded_files_info: list,
    input_row_count: int,
    user: dict,
    privacy_consent: bool = True,
    contact_consent: bool = True
) -> dict:
    analytics = full_result.get("analytics") or build_analytics_from_line_items(full_result)
    errors = full_result.get("errors", [])
    error_summary = full_result.get("error_summary") or build_error_summary(errors)
    errors_preview = full_result.get("errors_preview") or errors[:50]

    return {
        "totals": full_result.get("totals", {}),
        "summary_by_scope": full_result.get("summary_by_scope", []),
        "summary_by_scope_category": full_result.get("summary_by_scope_category", []),
        "line_items": full_result.get("line_items", []),
        "uploaded_files": uploaded_files_info,
        "input_row_count": input_row_count,
        "privacy_consent": privacy_consent,
        "contact_consent": contact_consent,
        "user": user,
        "analytics": analytics,
        "top_categories_by_kgco2e": full_result.get("top_categories_by_kgco2e", []),
        "top_sites_by_kgco2e": full_result.get("top_sites_by_kgco2e", []),
        "plain_language_takeaways": full_result.get("plain_language_takeaways", []),
        "actionable_insights": full_result.get("actionable_insights", []),
        "confidence_score": full_result.get("confidence_score", {}),
        "methodology_summary": full_result.get("methodology_summary", {}),
        "factor_quality_summary": full_result.get("factor_quality_summary", []),
        "error_count": len(errors),
        "errors": errors,
        "errors_preview": errors_preview,
        "error_summary": error_summary,
        "issue_groups": full_result.get("issue_groups", []),
        "data_quality": full_result.get("data_quality", {}),
        "unmapped_categories": full_result.get("unmapped_categories", []),
        "factor_transparency": full_result.get("factor_transparency", []),
    }


# =========================
# LEAD LOGGING
# =========================
def send_lead_to_logger(user_data: dict, result: dict, uploaded_files_info: list):
    url = os.getenv("LEAD_LOGGER_URL")
    if not url:
        return

    totals = result.get("totals", {})
    analytics = result.get("analytics", {})
    confidence = result.get("confidence_score", {})

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
        "confidence_score": confidence.get("score", 0),
        "source": "website_calculator"
    }

    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Lead logger failed: {type(e).__name__}: {str(e)}")


# =========================
# EXCEL BUILDERS
# =========================
def write_df(writer, sheet_name: str, df: pd.DataFrame):
    sheet_name = sanitize_sheet_name(sheet_name)
    if df is None or df.empty:
        pd.DataFrame([{"message": "No data available"}]).to_excel(
            writer, sheet_name=sheet_name, index=False, startrow=6
        )
    else:
        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=6)


def build_overview_dataframe(result: dict, uploaded_files_info: list):
    totals = result.get("totals", {})
    confidence = result.get("confidence_score", {})
    dq = result.get("data_quality", {})

    return pd.DataFrame([{
        "total_kgCO2e": totals.get("total_kgCO2e", 0),
        "total_tCO2e": totals.get("total_tCO2e", 0),
        "input_row_count": result.get("input_row_count", 0),
        "uploaded_files_count": len(uploaded_files_info),
        "error_count": result.get("error_count", 0),
        "confidence_score": confidence.get("score", 0),
        "confidence_label": confidence.get("label", ""),
        "coverage_percent": dq.get("coverage_percent", 0),
        "valid_rows_percent": dq.get("valid_rows_percent", 0),
        "invalid_rows_percent": dq.get("invalid_rows_percent", 0),
        "privacy_consent": result.get("privacy_consent", False),
        "contact_consent": result.get("contact_consent", False),
    }])


def build_executive_summary_dataframe(result: dict):
    totals = result.get("totals", {})
    analytics = result.get("analytics", {})
    confidence = result.get("confidence_score", {})
    dq = result.get("data_quality", {})
    top_category = analytics.get("top_category_by_kgco2e", {})
    top_site = analytics.get("top_site_by_kgco2e", {})
    top_scope = (result.get("summary_by_scope") or [{}])[0]

    return pd.DataFrame([{
        "total_tCO2e": totals.get("total_tCO2e", 0),
        "total_kgCO2e": totals.get("total_kgCO2e", 0),
        "largest_scope": top_scope.get("scope", ""),
        "largest_scope_percent": top_scope.get("percent_of_total", 0),
        "top_category": top_category.get("category", ""),
        "top_category_kgCO2e": top_category.get("emissions_kgCO2e", 0),
        "top_site": top_site.get("site_name", ""),
        "top_site_kgCO2e": top_site.get("emissions_kgCO2e", 0),
        "confidence_score": confidence.get("score", 0),
        "confidence_label": confidence.get("label", ""),
        "coverage_percent": dq.get("coverage_percent", 0),
    }])


def write_summary_sheet(writer, result: dict):
    workbook = writer.book
    ws = workbook.create_sheet("Executive Summary")

    style_report_sheet(
        ws,
        title="GS Carbon Emissions Report",
        subtitle="Executive summary for uploaded datasets"
    )

    totals = result.get("totals", {})
    confidence = result.get("confidence_score", {})
    dq = result.get("data_quality", {})
    takeaways = result.get("plain_language_takeaways", [])
    insights = result.get("actionable_insights", [])
    issue_groups = result.get("issue_groups", [])
    scope_rows = result.get("summary_by_scope", [])
    top_categories = result.get("top_categories_by_kgco2e", [])

    ws["A6"] = "Metric"
    ws["B6"] = "Value"
    ws["A7"] = "Total emissions (tCO2e)"
    ws["B7"] = totals.get("total_tCO2e", 0)
    ws["A8"] = "Total emissions (kgCO2e)"
    ws["B8"] = totals.get("total_kgCO2e", 0)
    ws["A9"] = "Confidence score"
    ws["B9"] = confidence.get("score", 0)
    ws["A10"] = "Confidence label"
    ws["B10"] = confidence.get("label", "")
    ws["A11"] = "Coverage percent"
    ws["B11"] = dq.get("coverage_percent", 0)
    ws["A12"] = "Error count"
    ws["B12"] = result.get("error_count", 0)

    ws["D6"] = "Plain-language takeaways"
    takeaways_row = 7
    if takeaways:
        for item in takeaways:
            ws[f"D{takeaways_row}"] = item
            takeaways_row += 1
    else:
        ws["D7"] = "No takeaways available."

    ws["G6"] = "Actionable insights"
    insights_row = 7
    if insights:
        for item in insights:
            ws[f"G{insights_row}"] = item.get("title", "")
            ws[f"H{insights_row}"] = item.get("body", "")
            insights_row += 1
    else:
        ws["G7"] = "No insights available."

    ws["A15"] = "Scope"
    ws["B15"] = "kg CO2e"
    ws["C15"] = "% of total"
    for i, row in enumerate(scope_rows, start=16):
        ws[f"A{i}"] = row.get("scope")
        ws[f"B{i}"] = row.get("emissions_kgCO2e")
        ws[f"C{i}"] = row.get("percent_of_total")

    ws["E15"] = "Top category"
    ws["F15"] = "kg CO2e"
    for i, row in enumerate(top_categories, start=16):
        ws[f"E{i}"] = row.get("category")
        ws[f"F{i}"] = row.get("emissions_kgCO2e")

    ws["G15"] = "Issue group"
    ws["H15"] = "Severity"
    ws["I15"] = "Count"
    ws["J15"] = "Guidance"
    for i, row in enumerate(issue_groups, start=16):
        ws[f"G{i}"] = row.get("title")
        ws[f"H{i}"] = row.get("severity")
        ws[f"I{i}"] = row.get("count")
        ws[f"J{i}"] = row.get("guidance")

    auto_fit_columns(ws)


def write_charts_sheet(writer, result: dict):
    workbook = writer.book
    ws = workbook.create_sheet("Charts")

    style_report_sheet(
        ws,
        title="GS Carbon Emissions Report",
        subtitle="Visual summary of uploaded datasets"
    )

    scope_rows = result.get("summary_by_scope", [])
    cat_rows = result.get("summary_by_scope_category", [])

    ws["A6"] = "Scope"
    ws["B6"] = "kg CO2e"
    for i, row in enumerate(scope_rows, start=7):
        ws[f"A{i}"] = row.get("scope")
        ws[f"B{i}"] = row.get("emissions_kgCO2e")

    ws["D6"] = "Scope - Category"
    ws["E6"] = "kg CO2e"
    for i, row in enumerate(cat_rows, start=7):
        ws[f"D{i}"] = f"{row.get('scope')} - {row.get('category')}"
        ws[f"E{i}"] = row.get("emissions_kgCO2e")

    ws["J6"] = "Scope"
    ws["K6"] = "kg CO2e"
    for i, row in enumerate(scope_rows, start=7):
        ws[f"J{i}"] = row.get("scope")
        ws[f"K{i}"] = row.get("emissions_kgCO2e")

    ws["N6"] = "Scope - Category"
    ws["O6"] = "kg CO2e"
    for i, row in enumerate(cat_rows, start=7):
        ws[f"N{i}"] = f"{row.get('scope')} - {row.get('category')}"
        ws[f"O{i}"] = row.get("emissions_kgCO2e")

    if len(scope_rows) > 0:
        add_bar_chart(ws, "Emissions by Scope", 2, 1, 6, 6 + len(scope_rows), "G6")
        add_pie_chart(ws, "Scope Share", 11, 10, 6, 6 + len(scope_rows), "Q6")

    if len(cat_rows) > 0:
        add_bar_chart(ws, "Emissions by Scope and Category", 5, 4, 6, 6 + len(cat_rows), "G24")
        add_pie_chart(ws, "Category Share", 15, 14, 6, 6 + len(cat_rows), "Q24")

    auto_fit_columns(ws)


def build_excel_report(result: dict, uploaded_files_info: list) -> io.BytesIO:
    output = io.BytesIO()

    df_overview = build_overview_dataframe(result, uploaded_files_info)
    df_exec = build_executive_summary_dataframe(result)
    df_summary_scope = pd.DataFrame(result.get("summary_by_scope", []))
    df_summary_scope_cat = pd.DataFrame(result.get("summary_by_scope_category", []))
    df_results = pd.DataFrame(result.get("line_items", []))
    df_errors = pd.DataFrame(result.get("errors", []))
    df_error_summary = pd.DataFrame(result.get("error_summary", []))
    df_issue_groups = pd.DataFrame(result.get("issue_groups", []))
    df_uploaded_files = pd.DataFrame(uploaded_files_info)
    df_analytics = pd.DataFrame([{"metric": k, "value": str(v)} for k, v in (result.get("analytics", {}) or {}).items()])
    df_user = pd.DataFrame([result.get("user", {})])
    df_data_quality = pd.DataFrame([result.get("data_quality", {})]) if result.get("data_quality") else pd.DataFrame()
    df_unmapped_categories = pd.DataFrame(result.get("unmapped_categories", []))
    df_factor_transparency = pd.DataFrame(result.get("factor_transparency", []))
    df_factor_quality = pd.DataFrame(result.get("factor_quality_summary", []))
    df_confidence = pd.DataFrame([result.get("confidence_score", {})]) if result.get("confidence_score") else pd.DataFrame()
    df_methodology = pd.DataFrame([result.get("methodology_summary", {})]) if result.get("methodology_summary") else pd.DataFrame()
    df_takeaways = pd.DataFrame([{"takeaway": x} for x in result.get("plain_language_takeaways", [])])
    df_insights = pd.DataFrame(result.get("actionable_insights", []))
    df_top_categories = pd.DataFrame(result.get("top_categories_by_kgco2e", []))
    df_top_sites = pd.DataFrame(result.get("top_sites_by_kgco2e", []))

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        write_df(writer, "Overview", df_overview)
        write_df(writer, "Executive Summary Data", df_exec)
        write_df(writer, "Summary by Scope", df_summary_scope)
        write_df(writer, "Summary by Category", df_summary_scope_cat)
        write_df(writer, "Top Categories", df_top_categories)
        write_df(writer, "Top Sites", df_top_sites)
        write_df(writer, "Takeaways", df_takeaways)
        write_df(writer, "Insights", df_insights)
        write_df(writer, "Confidence", df_confidence)
        write_df(writer, "Data Quality", df_data_quality)
        write_df(writer, "Issue Groups", df_issue_groups)
        write_df(writer, "Errors", df_errors)
        write_df(writer, "Error Summary", df_error_summary)
        write_df(writer, "Uploaded Files", df_uploaded_files)
        write_df(writer, "Results", df_results)
        write_df(writer, "Analytics", df_analytics)
        write_df(writer, "Methodology", df_methodology)
        write_df(writer, "User Details", df_user)
        write_df(writer, "Unmapped Categories", df_unmapped_categories)
        write_df(writer, "Factor Transparency", df_factor_transparency)
        write_df(writer, "Factor Quality", df_factor_quality)

        workbook = writer.book

        for sheet_name in workbook.sheetnames:
            ws = workbook[sheet_name]
            style_report_sheet(ws)
            auto_fit_columns(ws)

        write_summary_sheet(writer, result)
        write_charts_sheet(writer, result)

    output.seek(0)
    return output


# =========================
# API ROUTES
# =========================
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


@app.post("/upload-calculate")
async def upload_calculate(
    files: Annotated[List[UploadFile], File(description="Upload one or more CSV/XLSX activity datasets")],
    privacy_consent: Annotated[str, Form(...)],
    contact_consent: Annotated[str, Form(...)],
    full_name: Annotated[str, Form(...)],
    email: Annotated[str, Form(...)],
    company_name: Annotated[str, Form(...)],
    phone_number: Annotated[str, Form(...)],
):
    try:
        validate_user_inputs(
            privacy_consent=privacy_consent,
            contact_consent=contact_consent,
            full_name=full_name,
            email=email,
            company_name=company_name,
            phone_number=phone_number,
        )

        user = build_user_object(full_name, email, company_name, phone_number)

        combined_df_clean, uploaded_files_info, file_level_errors = prepare_combined_dataframe(files)
        activities = combined_df_clean.to_dict(orient="records")

        full_result = safe_run_emissions_engine(activities)
        full_result = merge_file_errors_with_result(full_result, file_level_errors)

        api_result = build_api_response(
            full_result=full_result,
            uploaded_files_info=uploaded_files_info,
            input_row_count=len(combined_df_clean),
            user=user,
            privacy_consent=True,
            contact_consent=True
        )

        send_lead_to_logger(
            {
                "full_name": full_name,
                "email": email,
                "company_name": company_name,
                "phone_number": phone_number,
                "privacy_consent": True,
                "contact_consent": True
            },
            api_result,
            uploaded_files_info
        )

        return api_result

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
    phone_number: Annotated[str, Form(...)],
):
    try:
        validate_user_inputs(
            privacy_consent=privacy_consent,
            contact_consent=contact_consent,
            full_name=full_name,
            email=email,
            company_name=company_name,
            phone_number=phone_number,
        )

        user = build_user_object(full_name, email, company_name, phone_number)

        combined_df_clean, uploaded_files_info, file_level_errors = prepare_combined_dataframe(files)
        activities = combined_df_clean.to_dict(orient="records")

        full_result = safe_run_emissions_engine(activities)
        full_result = merge_file_errors_with_result(full_result, file_level_errors)

        api_result = build_api_response(
            full_result=full_result,
            uploaded_files_info=uploaded_files_info,
            input_row_count=len(combined_df_clean),
            user=user,
            privacy_consent=True,
            contact_consent=True
        )

        output = build_excel_report(api_result, uploaded_files_info)

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

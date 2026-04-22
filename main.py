
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



html 



<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>GSC Carbon Emissions Calculator</title>
  <style>
    :root {
      --bg: #f4f7f8;
      --card: #ffffff;
      --text: #1f2937;
      --muted: #64748b;
      --border: #e2e8f0;
      --brand: #14532d;
      --brand-2: #0f4c81;
      --success: #166534;
      --warning: #b45309;
      --danger: #b00020;
      --info: #1d4ed8;
      --pill: #eef2ff;
      --shadow: 0 6px 18px rgba(15, 23, 42, 0.06);
      --radius: 16px;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      padding: 32px 20px 60px;
      font-family: Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.55;
    }

    .container {
      max-width: 1180px;
      margin: 0 auto;
    }

    .hero {
      margin-bottom: 24px;
    }

    .hero h1 {
      margin: 0 0 8px;
      font-size: 34px;
      color: #0f172a;
    }

    .hero p {
      margin: 0;
      color: var(--muted);
      max-width: 850px;
    }

    .layout {
      display: grid;
      grid-template-columns: 1fr;
      gap: 20px;
    }

    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 22px;
      box-shadow: var(--shadow);
    }

    .card h2 {
      margin-top: 0;
      margin-bottom: 14px;
      font-size: 22px;
      color: #0f172a;
    }

    .card h3 {
      margin-top: 22px;
      margin-bottom: 10px;
      font-size: 18px;
      color: #0f172a;
    }

    .subtext {
      color: var(--muted);
      font-size: 14px;
    }

    .input-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 16px;
    }

    label {
      display: block;
      font-weight: 700;
      margin-bottom: 8px;
      color: #0f172a;
    }

    input[type="text"],
    input[type="email"],
    input[type="tel"],
    input[type="file"] {
      width: 100%;
      padding: 11px 12px;
      border: 1px solid #cbd5e1;
      border-radius: 10px;
      font-size: 15px;
      background: white;
    }

    .button-row {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 14px;
    }

    button {
      border: none;
      border-radius: 12px;
      padding: 12px 18px;
      font-size: 15px;
      font-weight: 700;
      cursor: pointer;
      transition: 0.2s ease;
    }

    button:hover {
      transform: translateY(-1px);
    }

    button:disabled {
      opacity: 0.65;
      cursor: not-allowed;
      transform: none;
    }

    .primary-btn {
      background: var(--brand);
      color: white;
    }

    .secondary-btn {
      background: var(--brand-2);
      color: white;
    }

    .tertiary-btn {
      background: #e2e8f0;
      color: #0f172a;
    }

    .status-box {
      padding: 14px 16px;
      border-radius: 12px;
      background: #f8fafc;
      border: 1px solid var(--border);
      font-weight: 700;
    }

    .status-success {
      color: var(--success);
    }

    .status-error {
      color: var(--danger);
    }

    .status-info {
      color: var(--info);
    }

    .status-warning {
      color: var(--warning);
    }

    .summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      gap: 14px;
      margin-top: 12px;
    }

    .metric-card {
      border: 1px solid var(--border);
      background: #f8fafc;
      border-radius: 14px;
      padding: 16px;
    }

    .metric-label {
      font-size: 13px;
      color: var(--muted);
      margin-bottom: 8px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }

    .metric-value {
      font-size: 26px;
      font-weight: 800;
      color: #0f172a;
      line-height: 1.15;
    }

    .metric-note {
      margin-top: 6px;
      font-size: 13px;
      color: var(--muted);
    }

    .two-col {
      display: grid;
      grid-template-columns: 1.1fr 0.9fr;
      gap: 18px;
    }

    .three-col {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 18px;
    }

    @media (max-width: 900px) {
      .two-col,
      .three-col {
        grid-template-columns: 1fr;
      }
    }

    .list-clean {
      margin: 0;
      padding-left: 18px;
    }

    .list-clean li {
      margin-bottom: 8px;
    }

    .pill {
      display: inline-block;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--pill);
      font-size: 12px;
      font-weight: 700;
      color: #3730a3;
      margin-right: 8px;
      margin-bottom: 8px;
    }

    .bar-row {
      margin-bottom: 14px;
    }

    .bar-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 6px;
      font-size: 14px;
      font-weight: 700;
    }

    .bar-track {
      width: 100%;
      height: 10px;
      background: #e5e7eb;
      border-radius: 999px;
      overflow: hidden;
    }

    .bar-fill {
      height: 100%;
      background: linear-gradient(90deg, #14532d, #0f4c81);
      border-radius: 999px;
    }

    .insight-box {
      border: 1px solid var(--border);
      border-left: 5px solid var(--brand-2);
      background: #f8fbff;
      border-radius: 12px;
      padding: 14px 16px;
      margin-bottom: 12px;
    }

    .issue-group {
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px 16px;
      margin-bottom: 12px;
      background: #fff;
    }

    .issue-header {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      flex-wrap: wrap;
      margin-bottom: 8px;
    }

    .severity {
      display: inline-block;
      padding: 5px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      color: white;
    }

    .severity-high {
      background: #b00020;
    }

    .severity-medium {
      background: #b45309;
    }

    .severity-low {
      background: #2563eb;
    }

    .file-list,
    .raw-box {
      background: #f8fafc;
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px;
    }

    .raw-box {
      white-space: pre-wrap;
      overflow-x: auto;
      max-height: 480px;
      font-size: 13px;
    }

    .privacy-card {
      border-left: 6px solid var(--brand-2);
      background: #f8fbff;
    }

    .checkbox-row {
      display: flex;
      align-items: flex-start;
      gap: 10px;
      margin-top: 14px;
    }

    .checkbox-row input {
      margin-top: 3px;
      transform: scale(1.1);
    }

    .small-text {
      font-size: 14px;
      color: var(--muted);
    }

    .muted {
      color: var(--muted);
    }

    .section-divider {
      height: 1px;
      background: var(--border);
      margin: 18px 0;
    }

    .footer-note {
      color: var(--muted);
      font-size: 13px;
      margin-top: 12px;
    }

    .hidden {
      display: none;
    }

    .empty-state {
      color: var(--muted);
      font-style: italic;
    }

    .top-banner {
      background: linear-gradient(135deg, #ecfdf5, #eff6ff);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 18px 20px;
      margin-bottom: 20px;
      box-shadow: var(--shadow);
    }

    .top-banner strong {
      color: #0f172a;
    }

    .method-box {
      background: #f8fafc;
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px 16px;
      margin-bottom: 12px;
    }

    .download-note {
      color: var(--muted);
      font-size: 14px;
      margin-top: 10px;
    }

    .score-ring {
      width: 130px;
      height: 130px;
      border-radius: 50%;
      display: grid;
      place-items: center;
      margin: 6px auto 14px;
      background: conic-gradient(#14532d var(--score-deg, 0deg), #e5e7eb 0deg);
      position: relative;
    }

    .score-ring::before {
      content: "";
      position: absolute;
      inset: 12px;
      border-radius: 50%;
      background: white;
    }

    .score-ring-inner {
      position: relative;
      z-index: 1;
      text-align: center;
    }

    .score-ring-value {
      font-size: 28px;
      font-weight: 800;
      color: #0f172a;
      line-height: 1;
    }

    .score-ring-label {
      font-size: 12px;
      color: var(--muted);
      margin-top: 4px;
      font-weight: 700;
      text-transform: uppercase;
    }

    .badge-row {
      margin-top: 10px;
    }

    .scope-table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
    }

    .scope-table th,
    .scope-table td {
      text-align: left;
      padding: 10px 8px;
      border-bottom: 1px solid var(--border);
      vertical-align: top;
      font-size: 14px;
    }

    .scope-table th {
      color: var(--muted);
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }

    .note-card {
      border: 1px dashed #cbd5e1;
      border-radius: 12px;
      padding: 14px 16px;
      background: #fafafa;
      color: var(--muted);
      font-size: 14px;
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="hero">
      <h1>Carbon Emissions Calculator</h1>
      <p>
        Upload one or more CSV or Excel activity datasets to generate a consolidated carbon emissions report,
        including executive summary, scope breakdown, key insights, data quality indicators, and downloadable output.
      </p>
    </div>

    <div class="top-banner">
      <strong>Client-ready reporting mode enabled.</strong>
      This version is designed to present results in a more business-friendly format, with plain-language interpretation,
      data quality visibility, and clearer reporting outputs.
    </div>

    <div class="layout">
      <div class="card">
        <h2>User Details</h2>
        <div class="input-grid">
          <div>
            <label for="fullName">Full Name</label>
            <input id="fullName" type="text" placeholder="Your full name" />
          </div>
          <div>
            <label for="email">Work Email</label>
            <input id="email" type="email" placeholder="your.name@company.com" />
          </div>
          <div>
            <label for="companyName">Company Name</label>
            <input id="companyName" type="text" placeholder="Your company name" />
          </div>
          <div>
            <label for="phoneNumber">Phone Number</label>
            <input id="phoneNumber" type="tel" placeholder="Your phone number" />
          </div>
        </div>
      </div>

      <div class="card">
        <h2>Upload Activity Data</h2>
        <label for="files">Select one or more datasets</label>
        <input id="files" type="file" multiple accept=".csv,.xlsx,.xls" />
        <div class="button-row">
          <button id="uploadBtn" class="primary-btn" type="button">Upload and Calculate</button>
          <button id="downloadBtn" class="secondary-btn" type="button">Download Full Report</button>
          <button id="clearBtn" class="tertiary-btn" type="button">Clear Results</button>
        </div>
        <div class="download-note">
          The downloadable report can later be upgraded to a board-ready Excel or PDF export once the backend report generator is updated.
        </div>
      </div>

      <div class="card privacy-card">
        <h2>Privacy Notice</h2>
        <p>
          This tool is owned and copyrighted to GSC only, and processes uploaded datasets solely to validate inputs,
          calculate emissions, generate summaries, and produce downloadable reports according to GHG Protocol and DEFRA 2025.
        </p>

        <ul class="list-clean">
          <li><strong>What is uploaded:</strong> operational emissions datasets such as fuel, electricity, water, site, and reporting-period data.</li>
          <li><strong>What it is used for:</strong> emissions calculations, summary analytics, charts, and report generation.</li>
          <li><strong>Storage:</strong> uploaded files are processed to generate the report and are not intentionally retained after processing unless explicitly agreed.</li>
          <li><strong>Control:</strong> data processing is carried out on behalf of the client organisation for emissions reporting purposes.</li>
          <li><strong>Contact details:</strong> the details entered on this page may be used to contact you regarding your emissions report and related sustainability services.</li>
          <li><strong>Data minimisation:</strong> please upload operational emissions data only and avoid unnecessary personal data where possible.</li>
        </ul>

        <div class="checkbox-row">
          <input type="checkbox" id="privacyConsent" />
          <label for="privacyConsent" style="margin:0;font-weight:600;">
            I confirm that I understand this privacy notice and agree to the processing of the uploaded dataset for emissions reporting and report generation.
          </label>
        </div>

        <div class="checkbox-row">
          <input type="checkbox" id="contactConsent" />
          <label for="contactConsent" style="margin:0;font-weight:600;">
            I agree that GSC may contact me regarding my emissions report and related sustainability services.
          </label>
        </div>

        <p class="small-text">
          By continuing, you confirm that the uploaded files are appropriate for this purpose and that any personal data included is necessary and proportionate.
        </p>
      </div>

      <div class="card">
        <h2>Status</h2>
        <div id="status" class="status-box">Waiting for upload...</div>
      </div>

      <div id="resultsSection" class="hidden">
        <div class="card">
          <h2>Executive Summary</h2>
          <p class="subtext">
            A concise overview of total emissions, scope contribution, leading categories, and key takeaways in plain language.
          </p>

          <div id="executiveSummaryMetrics" class="summary-grid">
            <div class="metric-card">
              <div class="metric-label">Total emissions</div>
              <div class="metric-value">—</div>
              <div class="metric-note">Awaiting calculation.</div>
            </div>
          </div>

          <div class="section-divider"></div>

          <div class="two-col">
            <div>
              <h3>Scope Breakdown</h3>
              <div id="scopeBreakdown">
                <div class="empty-state">No scope breakdown available yet.</div>
              </div>
            </div>
            <div>
              <h3>Key Takeaways</h3>
              <div id="keyTakeaways">
                <div class="note-card">No takeaways available yet.</div>
              </div>
            </div>
          </div>
        </div>

        <div class="card">
          <h2>Actionable Insights</h2>
          <p class="subtext">
            Automated interpretation designed to highlight largest drivers, reduction opportunities, and notable patterns.
          </p>
          <div id="actionableInsights">
            <div class="empty-state">No insights available yet.</div>
          </div>
        </div>

        <div class="card">
          <h2>Data Quality and Confidence</h2>
          <div class="two-col">
            <div>
              <div id="confidencePanel">
                <div class="empty-state">No confidence metrics available yet.</div>
              </div>
            </div>
            <div>
              <h3>Data Quality Summary</h3>
              <div id="dataQualityPanel">
                <div class="empty-state">No data quality summary available yet.</div>
              </div>
            </div>
          </div>
        </div>

        <div class="card">
          <h2>Issues and Error Guidance</h2>
          <p class="subtext">
            Issues are grouped by type and prioritised by likely impact on reporting completeness and reliability.
          </p>
          <div id="issuesPanel">
            <div class="empty-state">No issues recorded.</div>
          </div>
        </div>

        <div class="card">
          <h2>Methodology Summary</h2>
          <div id="methodologyPanel">
            <div class="method-box">
              <strong>Methodology overview</strong>
              <p class="muted" style="margin-bottom:0;">
                Emissions are calculated using uploaded activity data, mapped to applicable emissions factors,
                and aggregated into total CO2e outputs aligned to reporting scopes where available.
              </p>
            </div>
            <div class="method-box">
              <strong>Assumptions and transparency</strong>
              <p class="muted" style="margin-bottom:0;">
                Where exact category or factor matching is not possible, the system may rely on inferred mappings,
                estimated classifications, or best-available factors. These should be reviewed before formal reporting.
              </p>
            </div>
          </div>
        </div>

        <div class="card">
          <h2>Uploaded Files</h2>
          <div id="fileSummary" class="file-list">No uploaded files processed yet.</div>
        </div>

        <div class="card">
          <h2>Technical Response</h2>
          <p class="subtext">
            This section is primarily for internal review and debugging. It should eventually be hidden from end clients in production.
          </p>
          <div id="rawResponse" class="raw-box">No response yet.</div>
        </div>
      </div>
    </div>
  </div>

  <script>
    const API_BASE = "https://carbon-emissions-api-2ua9.onrender.com";
    const CALCULATE_URL = `${API_BASE}/upload-calculate`;
    const DOWNLOAD_URL = `${API_BASE}/upload-calculate-download`;

    const fileInput = document.getElementById("files");
    const uploadBtn = document.getElementById("uploadBtn");
    const downloadBtn = document.getElementById("downloadBtn");
    const clearBtn = document.getElementById("clearBtn");

    const privacyConsentCheckbox = document.getElementById("privacyConsent");
    const contactConsentCheckbox = document.getElementById("contactConsent");

    const fullNameInput = document.getElementById("fullName");
    const emailInput = document.getElementById("email");
    const companyNameInput = document.getElementById("companyName");
    const phoneNumberInput = document.getElementById("phoneNumber");

    const statusBox = document.getElementById("status");
    const resultsSection = document.getElementById("resultsSection");
    const executiveSummaryMetrics = document.getElementById("executiveSummaryMetrics");
    const scopeBreakdown = document.getElementById("scopeBreakdown");
    const keyTakeaways = document.getElementById("keyTakeaways");
    const actionableInsights = document.getElementById("actionableInsights");
    const confidencePanel = document.getElementById("confidencePanel");
    const dataQualityPanel = document.getElementById("dataQualityPanel");
    const issuesPanel = document.getElementById("issuesPanel");
    const methodologyPanel = document.getElementById("methodologyPanel");
    const fileSummaryBox = document.getElementById("fileSummary");
    const rawResponseBox = document.getElementById("rawResponse");

    function setStatus(message, type = "info") {
      statusBox.className = "status-box";
      if (type === "success") {
        statusBox.innerHTML = `<span class="status-success">${message}</span>`;
      } else if (type === "error") {
        statusBox.innerHTML = `<span class="status-error">${message}</span>`;
      } else if (type === "warning") {
        statusBox.innerHTML = `<span class="status-warning">${message}</span>`;
      } else {
        statusBox.innerHTML = `<span class="status-info">${message}</span>`;
      }
    }

    function escapeHtml(value) {
      if (value === null || value === undefined) return "";
      return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
    }

    function formatNumber(value, decimals = 2) {
      const num = Number(value);
      if (!isFinite(num)) return "—";
      return num.toLocaleString(undefined, {
        minimumFractionDigits: 0,
        maximumFractionDigits: decimals
      });
    }

    function formatPercent(value, decimals = 1) {
      const num = Number(value);
      if (!isFinite(num)) return "—";
      return `${num.toFixed(decimals)}%`;
    }

    function toArray(value) {
      return Array.isArray(value) ? value : [];
    }

    function safeNumber(value, fallback = 0) {
      const num = Number(value);
      return isFinite(num) ? num : fallback;
    }

    function validateUserDetails() {
      const fullName = fullNameInput.value.trim();
      const email = emailInput.value.trim();
      const companyName = companyNameInput.value.trim();
      const phoneNumber = phoneNumberInput.value.trim();

      if (!fullName) {
        setStatus("Please enter your full name.", "error");
        return false;
      }

      if (!email) {
        setStatus("Please enter your work email.", "error");
        return false;
      }

      if (!companyName) {
        setStatus("Please enter your company name.", "error");
        return false;
      }

      if (!phoneNumber) {
        setStatus("Please enter your phone number.", "error");
        return false;
      }

      if (!privacyConsentCheckbox.checked) {
        setStatus("Please confirm the privacy notice before continuing.", "error");
        return false;
      }

      if (!contactConsentCheckbox.checked) {
        setStatus("Please agree to contact consent before continuing.", "error");
        return false;
      }

      return true;
    }

    function buildFormData() {
      if (!validateUserDetails()) return null;

      const files = fileInput.files;

      if (!files || files.length === 0) {
        setStatus("Please select at least one file.", "error");
        return null;
      }

      const formData = new FormData();

      for (const file of files) {
        formData.append("files", file);
      }

      formData.append("privacy_consent", "true");
      formData.append("contact_consent", "true");
      formData.append("full_name", fullNameInput.value.trim());
      formData.append("email", emailInput.value.trim());
      formData.append("company_name", companyNameInput.value.trim());
      formData.append("phone_number", phoneNumberInput.value.trim());

      return formData;
    }

    function calculateScopePercentages(scopeSummary, totalKg) {
      return scopeSummary.map(item => {
        const emissionsKg = safeNumber(item.emissions_kgCO2e);
        const percent = totalKg > 0 ? (emissionsKg / totalKg) * 100 : 0;
        return {
          ...item,
          calculated_percent: percent
        };
      });
    }

    function deriveConfidenceScore(data) {
      const dq = data.data_quality || {};
      const totalRows = safeNumber(dq.total_rows || data.input_row_count);
      const successfulRows = safeNumber(dq.successful_rows, totalRows > 0 ? totalRows - safeNumber(dq.errored_rows) : 0);
      const erroredRows = safeNumber(dq.errored_rows || data.error_count);
      const mappedRows = safeNumber(dq.mapped_category_rows);
      const inferredRows = safeNumber(dq.inferred_category_rows);
      const exactRows = safeNumber(dq.exact_factor_rows);
      const estimatedRows = safeNumber(dq.estimated_rows);

      let score = 100;

      if (totalRows > 0) {
        score -= (erroredRows / totalRows) * 45;
        if (mappedRows > 0 || inferredRows > 0) {
          const inferredRatio = inferredRows / Math.max(mappedRows + inferredRows, 1);
          score -= inferredRatio * 15;
        }
        if (estimatedRows > 0) {
          score -= (estimatedRows / totalRows) * 20;
        }
        if (exactRows > 0 && exactRows <= totalRows) {
          score += (exactRows / totalRows) * 5;
        }
      } else {
        score = 0;
      }

      score = Math.max(0, Math.min(100, Math.round(score)));

      let label = "Low";
      if (score >= 80) label = "High";
      else if (score >= 60) label = "Moderate";

      return { score, label };
    }

    function getConfidenceExplanation(scoreObj, dq) {
      const coverage = safeNumber(dq.coverage_percent);
      const erroredRows = safeNumber(dq.errored_rows);
      const inferredRows = safeNumber(dq.inferred_category_rows);
      const estimatedRows = safeNumber(dq.estimated_rows);

      const reasons = [];

      if (coverage >= 90) reasons.push("Most uploaded data was processed successfully.");
      else if (coverage > 0) reasons.push("Some rows could not be processed and should be reviewed.");

      if (erroredRows > 0) reasons.push(`${erroredRows} row(s) produced errors that may affect completeness.`);
      if (inferredRows > 0) reasons.push(`${inferredRows} row(s) relied on inferred category mapping.`);
      if (estimatedRows > 0) reasons.push(`${estimatedRows} row(s) appear to use estimated rather than exact calculation logic.`);

      if (reasons.length === 0) {
        reasons.push("No major quality limitations were detected in the returned response.");
      }

      return reasons;
    }

    function deriveTopCategories(data) {
      if (Array.isArray(data.top_categories_by_kgco2e)) return data.top_categories_by_kgco2e;
      if (Array.isArray(data.analytics?.top_categories_by_kgco2e)) return data.analytics.top_categories_by_kgco2e;

      const single = data.analytics?.top_category_by_kgco2e;
      return single ? [single] : [];
    }

    function deriveTopSites(data) {
      if (Array.isArray(data.top_sites_by_kgco2e)) return data.top_sites_by_kgco2e;
      if (Array.isArray(data.analytics?.top_sites_by_kgco2e)) return data.analytics.top_sites_by_kgco2e;

      const single = data.analytics?.top_site_by_kgco2e;
      return single ? [single] : [];
    }

    function deriveEstimatedVsExact(data) {
      const dq = data.data_quality || {};
      const totalRows = safeNumber(dq.total_rows || data.input_row_count);

      const exactRows = safeNumber(dq.exact_factor_rows);
      const estimatedRows = safeNumber(dq.estimated_rows);

      if (exactRows || estimatedRows) {
        return {
          exactRows,
          estimatedRows,
          exactPercent: totalRows > 0 ? (exactRows / totalRows) * 100 : 0,
          estimatedPercent: totalRows > 0 ? (estimatedRows / totalRows) * 100 : 0
        };
      }

      const inferredRows = safeNumber(dq.inferred_category_rows);
      const successfulRows = safeNumber(dq.successful_rows);

      return {
        exactRows: Math.max(successfulRows - inferredRows, 0),
        estimatedRows: inferredRows,
        exactPercent: totalRows > 0 ? ((Math.max(successfulRows - inferredRows, 0)) / totalRows) * 100 : 0,
        estimatedPercent: totalRows > 0 ? (inferredRows / totalRows) * 100 : 0
      };
    }

    function deriveKeyTakeaways(data) {
      const totals = data.totals || {};
      const totalKg = safeNumber(totals.total_kgCO2e);
      const totalT = safeNumber(totals.total_tCO2e);
      const scopes = calculateScopePercentages(toArray(data.summary_by_scope), totalKg).sort((a, b) => safeNumber(b.emissions_kgCO2e) - safeNumber(a.emissions_kgCO2e));
      const topCategories = deriveTopCategories(data);
      const topSites = deriveTopSites(data);
      const dq = data.data_quality || {};
      const coverage = safeNumber(dq.coverage_percent);
      const errors = safeNumber(data.error_count || dq.errored_rows);

      const takeaways = [];

      if (totalKg > 0 || totalT > 0) {
        takeaways.push(`Total reported emissions are ${formatNumber(totalT, 2)} tCO2e (${formatNumber(totalKg, 0)} kgCO2e).`);
      }

      if (scopes.length > 0) {
        const topScope = scopes[0];
        takeaways.push(`${topScope.scope || "Leading scope"} is the largest contributor, representing ${formatPercent(topScope.calculated_percent)} of total emissions.`);
      }

      if (topCategories.length > 0) {
        const topCat = topCategories[0];
        takeaways.push(`The highest-emitting category is ${topCat.category || "the leading category"}, contributing ${formatNumber(topCat.emissions_kgCO2e, 0)} kgCO2e.`);
      }

      if (topSites.length > 0) {
        const topSite = topSites[0];
        takeaways.push(`The largest site contributor is ${topSite.site_name || "the leading site"} with ${formatNumber(topSite.emissions_kgCO2e, 0)} kgCO2e.`);
      }

      if (coverage > 0) {
        takeaways.push(`Data processing coverage is ${formatPercent(coverage)}, indicating the share of uploaded rows successfully included in the output.`);
      }

      if (errors > 0) {
        takeaways.push(`${errors} issue(s) were identified and should be reviewed before relying on the report for external disclosure.`);
      }

      return takeaways.slice(0, 5);
    }

    function deriveInsights(data) {
      const totals = data.totals || {};
      const totalKg = safeNumber(totals.total_kgCO2e);
      const scopes = calculateScopePercentages(toArray(data.summary_by_scope), totalKg).sort((a, b) => safeNumber(b.emissions_kgCO2e) - safeNumber(a.emissions_kgCO2e));
      const topCategories = deriveTopCategories(data);
      const topSites = deriveTopSites(data);
      const dq = data.data_quality || {};
      const estimatedVsExact = deriveEstimatedVsExact(data);
      const unmappedCategories = toArray(data.unmapped_categories);

      const insights = [];

      if (scopes.length > 0) {
        const topScope = scopes[0];
        insights.push({
          title: "Largest emissions driver",
          body: `${topScope.scope || "Leading scope"} is currently the biggest contributor at ${formatNumber(topScope.emissions_kgCO2e, 0)} kgCO2e, or ${formatPercent(topScope.calculated_percent)} of the total. This is the first area to investigate for reduction planning.`
        });
      }

      if (topCategories.length > 0) {
        const cat = topCategories[0];
        insights.push({
          title: "Priority reduction opportunity",
          body: `The category with the greatest impact is ${cat.category || "the top category"}. Targeting this activity area could create the largest near-term reduction effect.`
        });
      }

      if (topSites.length > 0) {
        const site = topSites[0];
        insights.push({
          title: "Operational hotspot",
          body: `${site.site_name || "A leading site"} appears to be the main site-level contributor. A site-specific review could help identify concentrated reduction opportunities.`
        });
      }

      if (safeNumber(dq.coverage_percent) < 90 && safeNumber(dq.total_rows || data.input_row_count) > 0) {
        insights.push({
          title: "Data completeness risk",
          body: `Coverage is currently ${formatPercent(dq.coverage_percent)}. This suggests some records were excluded, which may reduce report completeness until issues are resolved.`
        });
      }

      if (estimatedVsExact.estimatedRows > 0) {
        insights.push({
          title: "Estimated calculations present",
          body: `${estimatedVsExact.estimatedRows} row(s) appear to rely on estimated or inferred logic rather than exact factor matching. These should be reviewed before formal client or audit use.`
        });
      }

      if (unmappedCategories.length > 0) {
        const topUnmapped = unmappedCategories[0];
        insights.push({
          title: "Category mapping improvement needed",
          body: `Some categories remain unmapped. For example, ${topUnmapped.category || "an uploaded category"} appears ${topUnmapped.count || 0} time(s). Improving category mapping will increase confidence and reporting accuracy.`
        });
      }

      return insights.slice(0, 6);
    }

    function deriveIssueGroups(data) {
      if (Array.isArray(data.issue_groups) && data.issue_groups.length > 0) {
        return data.issue_groups;
      }

      const groups = [];
      const errorSummary = toArray(data.error_summary);
      const errorsPreview = toArray(data.errors_preview);
      const uploadedFiles = toArray(data.uploaded_files);
      const unmappedCategories = toArray(data.unmapped_categories);

      if (errorSummary.length > 0) {
        errorSummary.forEach(item => {
          const message = item.error || "Unknown error";
          const count = safeNumber(item.count);
          const severity = count >= 20 ? "high" : count >= 5 ? "medium" : "low";
          groups.push({
            title: message,
            severity,
            count,
            guidance: inferGuidanceFromError(message),
            examples: errorsPreview
              .filter(e => (e.error || "").toLowerCase() === message.toLowerCase())
              .slice(0, 3)
              .map(e => buildErrorExample(e))
          });
        });
      }

      uploadedFiles.forEach(file => {
        if (Array.isArray(file.missing_required_columns) && file.missing_required_columns.length > 0) {
          groups.push({
            title: `Missing required columns in ${file.filename}`,
            severity: "high",
            count: file.missing_required_columns.length,
            guidance: `Add the required columns before upload: ${file.missing_required_columns.join(", ")}.`,
            examples: []
          });
        }
      });

      if (unmappedCategories.length > 0) {
        groups.push({
          title: "Unmapped categories detected",
          severity: "medium",
          count: unmappedCategories.length,
          guidance: "Review category mapping rules so these activity labels can be matched to the correct emissions factors.",
          examples: unmappedCategories.slice(0, 5).map(item => `${item.category}: ${item.count}`)
        });
      }

      return groups;
    }

    function inferGuidanceFromError(message) {
      const msg = String(message || "").toLowerCase();

      if (msg.includes("missing")) {
        return "Check that all required fields and columns are present in the uploaded file.";
      }
      if (msg.includes("unit")) {
        return "Review the unit values and ensure they match expected measurement formats.";
      }
      if (msg.includes("factor")) {
        return "Check the category, unit, and source mapping to ensure the correct emissions factor can be selected.";
      }
      if (msg.includes("category")) {
        return "Review category labels and align them to the allowed mapping taxonomy used by the calculator.";
      }
      if (msg.includes("date")) {
        return "Validate date fields and ensure reporting periods are in a consistent format.";
      }
      if (msg.includes("numeric") || msg.includes("number")) {
        return "Check activity quantities for blanks, text values, or invalid numeric formatting.";
      }

      return "Review the source rows shown in the examples and correct the input data before re-uploading.";
    }

    function buildErrorExample(err) {
      const source = err.source_file ? `[${err.source_file}] ` : "";
      const rowPart = err.row_index !== undefined && err.row_index !== null
        ? `Row ${err.row_index}: `
        : err.row_index_original !== undefined && err.row_index_original !== null
        ? `Original row ${err.row_index_original}: `
        : "";
      return `${source}${rowPart}${err.error || "Unknown error"}`;
    }

    function renderExecutiveSummary(data) {
      const totals = data.totals || {};
      const totalKg = safeNumber(totals.total_kgCO2e);
      const totalT = safeNumber(totals.total_tCO2e);
      const dq = data.data_quality || {};
      const confidence = deriveConfidenceScore(data);
      const scopeSummary = calculateScopePercentages(toArray(data.summary_by_scope), totalKg)
        .sort((a, b) => safeNumber(b.emissions_kgCO2e) - safeNumber(a.emissions_kgCO2e));
      const topCategories = deriveTopCategories(data);
      const inputRowCount = safeNumber(data.input_row_count || dq.total_rows);
      const errorCount = safeNumber(data.error_count || dq.errored_rows);

      executiveSummaryMetrics.innerHTML = `
        <div class="metric-card">
          <div class="metric-label">Total Emissions</div>
          <div class="metric-value">${formatNumber(totalT, 2)} tCO2e</div>
          <div class="metric-note">${formatNumber(totalKg, 0)} kgCO2e</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">Rows Processed</div>
          <div class="metric-value">${formatNumber(inputRowCount, 0)}</div>
          <div class="metric-note">Uploaded input rows</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">Confidence Score</div>
          <div class="metric-value">${confidence.score}/100</div>
          <div class="metric-note">${confidence.label} confidence</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">Issues Found</div>
          <div class="metric-value">${formatNumber(errorCount, 0)}</div>
          <div class="metric-note">Rows or items needing review</div>
        </div>
      `;

      if (scopeSummary.length > 0) {
        let html = `
          <table class="scope-table">
            <thead>
              <tr>
                <th>Scope</th>
                <th>Emissions</th>
                <th>Share</th>
              </tr>
            </thead>
            <tbody>
        `;

        scopeSummary.forEach(scope => {
          html += `
            <tr>
              <td>${escapeHtml(scope.scope || "Unspecified")}</td>
              <td>${formatNumber(scope.emissions_tCO2e, 2)} tCO2e</td>
              <td>${formatPercent(scope.calculated_percent)}</td>
            </tr>
          `;
        });

        html += `</tbody></table><div style="margin-top:14px;">`;

        scopeSummary.forEach(scope => {
          html += `
            <div class="bar-row">
              <div class="bar-head">
                <span>${escapeHtml(scope.scope || "Unspecified")}</span>
                <span>${formatPercent(scope.calculated_percent)}</span>
              </div>
              <div class="bar-track">
                <div class="bar-fill" style="width:${Math.max(0, Math.min(scope.calculated_percent, 100))}%;"></div>
              </div>
            </div>
          `;
        });

        html += `</div>`;
        scopeBreakdown.innerHTML = html;
      } else {
        scopeBreakdown.innerHTML = `<div class="empty-state">No scope summary was returned by the API.</div>`;
      }

      const takeaways = deriveKeyTakeaways(data);
      const topCategoryPills = topCategories.slice(0, 3).map(cat =>
        `<span class="pill">${escapeHtml(cat.category || "Category")} · ${formatNumber(cat.emissions_kgCO2e, 0)} kgCO2e</span>`
      ).join("");

      keyTakeaways.innerHTML = `
        ${topCategoryPills ? `<div class="badge-row">${topCategoryPills}</div>` : ""}
        <ul class="list-clean">
          ${takeaways.map(item => `<li>${escapeHtml(item)}</li>`).join("")}
        </ul>
      `;
    }

    function renderInsights(data) {
      const insights = deriveInsights(data);

      if (insights.length === 0) {
        actionableInsights.innerHTML = `<div class="empty-state">No automated insights could be generated from the current response.</div>`;
        return;
      }

      actionableInsights.innerHTML = insights.map(item => `
        <div class="insight-box">
          <strong>${escapeHtml(item.title)}</strong>
          <div style="margin-top:6px;">${escapeHtml(item.body)}</div>
        </div>
      `).join("");
    }

    function renderConfidenceAndQuality(data) {
      const dq = data.data_quality || {};
      const confidence = deriveConfidenceScore(data);
      const confidenceReasons = getConfidenceExplanation(confidence, dq);
      const estimatedVsExact = deriveEstimatedVsExact(data);

      const deg = Math.round((confidence.score / 100) * 360);

      confidencePanel.innerHTML = `
        <h3>Overall Confidence</h3>
        <div class="score-ring" style="--score-deg:${deg}deg;">
          <div class="score-ring-inner">
            <div class="score-ring-value">${confidence.score}</div>
            <div class="score-ring-label">${escapeHtml(confidence.label)}</div>
          </div>
        </div>
        <div class="method-box">
          <strong>What this means</strong>
          <ul class="list-clean" style="margin-top:8px;">
            ${confidenceReasons.map(reason => `<li>${escapeHtml(reason)}</li>`).join("")}
          </ul>
        </div>
        <div class="method-box">
          <strong>Estimated vs exact treatment</strong>
          <ul class="list-clean" style="margin-top:8px;">
            <li><strong>Exact / directly matched rows:</strong> ${formatNumber(estimatedVsExact.exactRows, 0)} (${formatPercent(estimatedVsExact.exactPercent)})</li>
            <li><strong>Estimated / inferred rows:</strong> ${formatNumber(estimatedVsExact.estimatedRows, 0)} (${formatPercent(estimatedVsExact.estimatedPercent)})</li>
          </ul>
        </div>
      `;

      const totalRows = safeNumber(dq.total_rows || data.input_row_count);
      const successfulRows = safeNumber(dq.successful_rows, totalRows > 0 ? totalRows - safeNumber(dq.errored_rows) : 0);
      const erroredRows = safeNumber(dq.errored_rows || data.error_count);
      const coverage = totalRows > 0 ? (successfulRows / totalRows) * 100 : safeNumber(dq.coverage_percent);
      const mappedRows = safeNumber(dq.mapped_category_rows);
      const inferredRows = safeNumber(dq.inferred_category_rows);

      dataQualityPanel.innerHTML = `
        <div class="metric-card" style="margin-bottom:12px;">
          <div class="metric-label">Coverage</div>
          <div class="metric-value">${formatPercent(coverage)}</div>
          <div class="metric-note">Successful rows as a share of total input</div>
        </div>

        <ul class="list-clean">
          <li><strong>Total rows:</strong> ${formatNumber(totalRows, 0)}</li>
          <li><strong>Valid / successful rows:</strong> ${formatNumber(successfulRows, 0)}</li>
          <li><strong>Invalid / errored rows:</strong> ${formatNumber(erroredRows, 0)}</li>
          <li><strong>Mapped category rows:</strong> ${formatNumber(mappedRows, 0)}</li>
          <li><strong>Inferred category rows:</strong> ${formatNumber(inferredRows, 0)}</li>
        </ul>
      `;
    }

    function renderIssues(data) {
      const groups = deriveIssueGroups(data);

      if (groups.length === 0) {
        issuesPanel.innerHTML = `<div class="empty-state">No grouped issues were returned or inferred from the current response.</div>`;
        return;
      }

      issuesPanel.innerHTML = groups.map(group => {
        const severity = String(group.severity || "low").toLowerCase();
        const severityClass =
          severity === "high" ? "severity-high" :
          severity === "medium" ? "severity-medium" :
          "severity-low";

        const examples = toArray(group.examples);

        return `
          <div class="issue-group">
            <div class="issue-header">
              <strong>${escapeHtml(group.title || "Issue")}</strong>
              <div>
                <span class="severity ${severityClass}">${escapeHtml(severity.toUpperCase())}</span>
              </div>
            </div>
            <div class="muted" style="margin-bottom:8px;">Count: ${formatNumber(group.count, 0)}</div>
            <div><strong>How to fix:</strong> ${escapeHtml(group.guidance || "Review the affected rows and correct the source data.")}</div>
            ${
              examples.length > 0
                ? `
                  <div style="margin-top:10px;"><strong>Examples</strong></div>
                  <ul class="list-clean" style="margin-top:6px;">
                    ${examples.map(ex => `<li>${escapeHtml(ex)}</li>`).join("")}
                  </ul>
                `
                : ""
            }
          </div>
        `;
      }).join("");
    }

    function renderMethodology(data) {
      const factors = toArray(data.factor_transparency);
      const factorPreview = factors.slice(0, 8);

      let html = `
        <div class="method-box">
          <strong>Readable methodology summary</strong>
          <p class="muted" style="margin-bottom:0;">
            This report combines uploaded operational activity data, applies emissions factors where matches are available,
            and calculates total greenhouse gas emissions in CO2e. Results are summarised by scope and category where supported by the input and mapping logic.
          </p>
        </div>

        <div class="method-box">
          <strong>Interpretation guidance</strong>
          <p class="muted" style="margin-bottom:0;">
            Figures should be interpreted alongside the data quality and confidence section. Where rows are inferred, estimated,
            or excluded due to input issues, the total may understate or approximate actual emissions until the source data is refined.
          </p>
        </div>
      `;

      if (factorPreview.length > 0) {
        html += `
          <div class="method-box">
            <strong>Emission factor examples</strong>
            <ul class="list-clean" style="margin-top:8px;">
              ${factorPreview.map(item => `
                <li>
                  <strong>${escapeHtml(item.factor_name || "Factor")}</strong>
                  ${
                    item.emission_factor_kgCO2e_per_unit !== undefined && item.emission_factor_kgCO2e_per_unit !== null
                      ? ` — ${escapeHtml(item.emission_factor_kgCO2e_per_unit)}`
                      : ""
                  }
                  ${escapeHtml(item.normalized_unit || "")}
                  ${item.data_source ? ` (${escapeHtml(item.data_source)})` : ""}
                </li>
              `).join("")}
            </ul>
          </div>
        `;
      }

      methodologyPanel.innerHTML = html;
    }

    function renderFiles(data) {
      const uploadedFiles = toArray(data.uploaded_files);

      if (uploadedFiles.length === 0) {
        fileSummaryBox.innerHTML = "No uploaded file details returned.";
        return;
      }

      fileSummaryBox.innerHTML = `
        <ul class="list-clean">
          ${uploadedFiles.map(file => `
            <li>
              <strong>${escapeHtml(file.filename || "Unnamed file")}</strong> —
              ${formatNumber(file.rows, 0)} rows
              ${
                Array.isArray(file.missing_required_columns) && file.missing_required_columns.length
                  ? ` — missing columns: ${escapeHtml(file.missing_required_columns.join(", "))}`
                  : ""
              }
            </li>
          `).join("")}
        </ul>
      `;
    }

    function renderAll(data) {
      resultsSection.classList.remove("hidden");
      renderExecutiveSummary(data);
      renderInsights(data);
      renderConfidenceAndQuality(data);
      renderIssues(data);
      renderMethodology(data);
      renderFiles(data);
      rawResponseBox.textContent = JSON.stringify(data, null, 2);
    }

    function resetResults() {
      resultsSection.classList.add("hidden");
      executiveSummaryMetrics.innerHTML = `
        <div class="metric-card">
          <div class="metric-label">Total emissions</div>
          <div class="metric-value">—</div>
          <div class="metric-note">Awaiting calculation.</div>
        </div>
      `;
      scopeBreakdown.innerHTML = `<div class="empty-state">No scope breakdown available yet.</div>`;
      keyTakeaways.innerHTML = `<div class="note-card">No takeaways available yet.</div>`;
      actionableInsights.innerHTML = `<div class="empty-state">No insights available yet.</div>`;
      confidencePanel.innerHTML = `<div class="empty-state">No confidence metrics available yet.</div>`;
      dataQualityPanel.innerHTML = `<div class="empty-state">No data quality summary available yet.</div>`;
      issuesPanel.innerHTML = `<div class="empty-state">No issues recorded.</div>`;
      methodologyPanel.innerHTML = `
        <div class="method-box">
          <strong>Methodology overview</strong>
          <p class="muted" style="margin-bottom:0;">
            Emissions are calculated using uploaded activity data, mapped to applicable emissions factors,
            and aggregated into total CO2e outputs aligned to reporting scopes where available.
          </p>
        </div>
      `;
      fileSummaryBox.textContent = "No uploaded files processed yet.";
      rawResponseBox.textContent = "No response yet.";
      setStatus("Waiting for upload...", "info");
    }

    async function uploadAndCalculate() {
      const formData = buildFormData();
      if (!formData) return;

      try {
        uploadBtn.disabled = true;
        downloadBtn.disabled = true;
        clearBtn.disabled = true;

        setStatus("Uploading files and generating report. Large or messy files may take longer.", "info");
        resultsSection.classList.remove("hidden");
        rawResponseBox.textContent = "Waiting for API response...";
        executiveSummaryMetrics.innerHTML = `
          <div class="metric-card">
            <div class="metric-label">Total emissions</div>
            <div class="metric-value">Processing...</div>
            <div class="metric-note">Please wait while the API completes the calculation.</div>
          </div>
        `;

        const response = await fetch(CALCULATE_URL, {
          method: "POST",
          body: formData
        });

        const responseText = await response.text();
        let data = null;

        try {
          data = JSON.parse(responseText);
        } catch (_) {
          data = { raw: responseText };
        }

        if (!response.ok) {
          const detail = data?.detail;
          setStatus(
            `API error: ${
              typeof detail === "string"
                ? detail
                : detail
                ? JSON.stringify(detail)
                : responseText || "Unknown server error"
            }`,
            "error"
          );
          rawResponseBox.textContent = typeof data === "object"
            ? JSON.stringify(data, null, 2)
            : String(responseText);
          return;
        }

        setStatus("Calculation completed successfully.", "success");
        renderAll(data);

      } catch (error) {
        setStatus(`Request failed: ${error.message}`, "error");
        rawResponseBox.textContent = String(error);
      } finally {
        uploadBtn.disabled = false;
        downloadBtn.disabled = false;
        clearBtn.disabled = false;
      }
    }

    async function uploadAndDownload() {
      const formData = buildFormData();
      if (!formData) return;

      try {
        uploadBtn.disabled = true;
        downloadBtn.disabled = true;
        clearBtn.disabled = true;

        setStatus("Generating downloadable report...", "info");

        const response = await fetch(DOWNLOAD_URL, {
          method: "POST",
          body: formData
        });

        if (!response.ok) {
          const responseText = await response.text();
          let data = null;

          try {
            data = JSON.parse(responseText);
          } catch (_) {
            data = { raw: responseText };
          }

          const detail = data?.detail;
          setStatus(
            `${
              typeof detail === "string"
                ? detail
                : detail
                ? JSON.stringify(detail)
                : responseText || "Error generating downloadable report."
            }`,
            "error"
          );

          rawResponseBox.textContent = typeof data === "object"
            ? JSON.stringify(data, null, 2)
            : String(responseText);
          return;
        }

        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "carbon_emissions_report.xlsx";
        document.body.appendChild(a);
        a.click();
        a.remove();
        window.URL.revokeObjectURL(url);

        setStatus("Download started successfully.", "success");

      } catch (error) {
        setStatus(`Download failed: ${error.message}`, "error");
        rawResponseBox.textContent = String(error);
      } finally {
        uploadBtn.disabled = false;
        downloadBtn.disabled = false;
        clearBtn.disabled = false;
      }
    }

    uploadBtn.addEventListener("click", uploadAndCalculate);
    downloadBtn.addEventListener("click", uploadAndDownload);
    clearBtn.addEventListener("click", resetResults);
  </script>
</body>
</html>



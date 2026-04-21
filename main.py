from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import List, Annotated, Optional
from io import BytesIO
import io
import os
import re
import requests
import pandas as pd
from datetime import datetime
import json

from openpyxl.drawing.image import Image as XLImage
from openpyxl.chart import BarChart, PieChart, Reference
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

# Import from the new production-ready engine
from engine import (
    run_emissions_engine,
    validate_and_preview_csv,
    generate_csv_template,
    get_factor_audit_log,
    get_supported_years,
    IntelligentColumnDetector,
    CSVValidator,
    CSVTemplateGenerator,
    FactorAuditLog,
    LeadGenFormatter,
    calculate_emissions_batch_safe,
    make_json_safe,
    DEFAULT_FACTOR_YEAR,
)

app = FastAPI(
    title="Carbon Emissions Calculator API",
    description="Production-ready carbon calculator with intelligent CSV parsing and DEFRA 2025 factors",
    version="2.0.0"
)


# =========================
# CORS CONFIGURATION
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://gscsustainability.co.uk",
        "https://www.gscsustainability.co.uk",
        "http://localhost:3000",
        "http://localhost:8000",
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
    return {
        "status": "ok",
        "message": "Carbon emissions API v2.0 - Production Ready",
        "version": "2.0.0",
        "features": [
            "Intelligent CSV parsing",
            "Pre-upload validation",
            "DEFRA 2025 factors",
            "Factor transparency audit logs",
            "Lead-generation integration",
            "Automated template generation"
        ]
    }


@app.head("/")
def root_head():
    return


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "2.0.0"
    }


@app.get("/debug-routes")
def debug_routes():
    return {
        "routes": [
            "/",
            "/health",
            "/api/calculate",
            "/api/upload-calculate",
            "/api/upload-calculate-download",
            "/api/validate-csv",
            "/api/generate-template",
            "/api/factor-audit-log",
            "/api/supported-years",
            "/api/detect-columns",
        ]
    }


# =========================
# HELPER FUNCTIONS
# =========================
def safe_string(value):
    """Safely convert value to string."""
    if value is None:
        return ""
    return str(value).strip()


def to_bool_string_true(value: str) -> bool:
    """Convert string 'true' to boolean."""
    return str(value).strip().lower() == "true"


def normalize_uploaded_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to lowercase."""
    df = df.copy()
    df.columns = [
        str(col).strip().lower().replace("\n", " ").replace("\r", " ")
        for col in df.columns
    ]
    return df


def sanitize_sheet_name(name: str) -> str:
    """Sanitize sheet name for Excel."""
    if not name:
        return "Sheet"
    cleaned = re.sub(r"[\\/*?:\[\]]", "", str(name)).strip()
    return cleaned[:31] if cleaned else "Sheet"


def auto_fit_columns(ws, min_width=12, max_width=45):
    """Auto-fit Excel column widths."""
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
    """Style Excel report sheet."""
    ws["A1"] = title
    ws["A2"] = subtitle
    ws["A3"] = "Aligned with GHG Protocol and DEFRA 2025 | Data-Driven Sustainability"

    ws["A1"].font = Font(size=16, bold=True, color="1F4E78")
    ws["A2"].font = Font(size=11, italic=True)
    ws["A3"].font = Font(size=10, color="666666")

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
    """Add bar chart to worksheet."""
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
    """Add pie chart to worksheet."""
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
    """Build summary of errors."""
    if not errors:
        return []

    df_errors = pd.DataFrame(errors)
    if "error" not in df_errors.columns:
        return []

    grouped = df_errors["error"].astype(str).value_counts().reset_index()
    grouped.columns = ["error", "count"]
    return grouped.to_dict(orient="records")


def validate_user_inputs(
    privacy_consent: str,
    contact_consent: str,
    full_name: str,
    email: str,
    company_name: str,
    phone_number: str,
):
    """Validate user consent and contact information."""
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
    """Build user data object."""
    return {
        "full_name": full_name,
        "email": email,
        "company_name": company_name,
        "phone_number": phone_number
    }


# =========================
# FILE READING & PROCESSING
# =========================
def read_uploaded_file(uploaded_file: UploadFile):
    """Read and parse uploaded CSV or Excel file."""
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
                file_warnings.append("CSV parsed in tolerant mode; some malformed lines may have been skipped.")
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
    """Combine multiple uploaded files and apply intelligent preprocessing."""
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

            # Add missing required columns
            missing_in_file = [col for col in required_columns if col not in df.columns]
            for col in missing_in_file:
                df[col] = ""

            if missing_in_file:
                file_level_errors.append({
                    "source_file": uploaded_file.filename,
                    "error": f"Missing required columns added as blank: {missing_in_file}",
                    "severity": "warning"
                })

            for warning in file_warnings:
                file_level_errors.append({
                    "source_file": uploaded_file.filename,
                    "error": warning,
                    "severity": "info"
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
                "error": f"{e.detail}",
                "severity": "error"
            })
        except Exception as e:
            file_level_errors.append({
                "source_file": uploaded_file.filename,
                "error": f"Failed to process file: {type(e).__name__}: {str(e)}",
                "severity": "error"
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

    # Apply intelligent column detection and mapping
    try:
        column_detection = IntelligentColumnDetector.auto_detect_columns(combined_df)
        
        # Auto-map columns based on detection
        for col, detection in column_detection.items():
            if detection["intent"].value == "amount" and "amount" not in combined_df.columns:
                combined_df = combined_df.rename(columns={col: "amount"})
            elif detection["intent"].value == "unit" and "unit" not in combined_df.columns:
                combined_df = combined_df.rename(columns={col: "unit"})
            elif detection["intent"].value == "category" and "category" not in combined_df.columns:
                combined_df = combined_df.rename(columns={col: "category"})
        
        combined_df_clean = combined_df.copy()
    except Exception as e:
        file_level_errors.append({
            "source_file": "intelligent_preprocessing",
            "error": f"Intelligent preprocessing applied with fallback: {type(e).__name__}",
            "severity": "warning"
        })
        combined_df_clean = combined_df.copy()

    # Ensure all required columns exist
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
# RESULT BUILDING
# =========================
def build_api_response(
    batch_result: dict,
    uploaded_files_info: list,
    company_name: str,
    reporting_period: str,
    user: dict,
    privacy_consent: bool = True,
    contact_consent: bool = True
) -> dict:
    """Build comprehensive API response."""
    
    # Generate lead-gen formatted data
    lead_gen_data = LeadGenFormatter.format_for_google_sheets(
        batch_result,
        company_name,
        reporting_period,
        user.get("email") if user else None
    )
    
    # Generate fleet audit
    fleet_audit = FactorAuditLog.generate_fleet_audit(batch_result)
    
    # Generate factor transparency
    factor_transparency = FactorAuditLog.generate_factor_transparency_report(batch_result)

    return {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "version": "2.0.0",
            "defra_year": DEFAULT_FACTOR_YEAR,
            "company_name": company_name,
            "reporting_period": reporting_period,
        },
        "totals": batch_result.get("totals", {}),
        "line_items": batch_result.get("report", pd.DataFrame()).to_dict(orient="records") if not batch_result.get("report", pd.DataFrame()).empty else [],
        "uploaded_files": uploaded_files_info,
        "user": user,
        "privacy_consent": privacy_consent,
        "contact_consent": contact_consent,
        "data_quality": batch_result.get("data_quality", {}),
        "errors": batch_result.get("errors", []),
        "error_summary": build_error_summary(batch_result.get("errors", [])),
        "errors_preview": batch_result.get("errors", [])[:50],
        "fleet_audit": fleet_audit,
        "factor_transparency": factor_transparency,
        "lead_gen_data": lead_gen_data,
    }


def merge_file_errors_with_result(base_result: dict, file_level_errors: list) -> dict:
    """Merge file-level errors with calculation result."""
    result = dict(base_result)
    combined_errors = file_level_errors + result.get("errors", [])
    result["errors"] = combined_errors
    result["error_summary"] = build_error_summary(combined_errors)
    result["errors_preview"] = combined_errors[:50]
    return result


# =========================
# LEAD LOGGING
# =========================
def send_lead_to_logger(user_data: dict, result: dict, uploaded_files_info: list):
    """Send lead data to external logger (Google Sheets, Zapier, etc)."""
    url = os.getenv("LEAD_LOGGER_URL")
    if not url:
        return

    totals = result.get("totals", {})
    lead_gen = result.get("lead_gen_data", {})
    lead_data = lead_gen.get("lead_data", {})

    payload = {
        "timestamp": datetime.now().isoformat(),
        "full_name": user_data.get("full_name", ""),
        "email": user_data.get("email", ""),
        "company_name": user_data.get("company_name", ""),
        "phone_number": user_data.get("phone_number", ""),
        "privacy_consent": user_data.get("privacy_consent", False),
        "contact_consent": user_data.get("contact_consent", False),
        "uploaded_files_count": len(uploaded_files_info),
        "total_co2e_tonnes": lead_data.get("total_co2e_tonnes", 0),
        "total_co2e_kgco2e": lead_data.get("total_co2e_kgCO2e", 0),
        "data_quality_score": lead_data.get("data_quality_score", 0),
        "rows_processed": lead_data.get("rows_processed", 0),
        "reporting_period": lead_data.get("reporting_period", ""),
        "is_valid_for_reporting": lead_data.get("is_valid_for_reporting", False),
        "scope_breakdown": lead_gen.get("breakdown_by_scope", {}),
        "category_breakdown": lead_gen.get("breakdown_by_category", {}),
        "source": "website_calculator_v2"
    }

    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Lead logger failed: {type(e).__name__}: {str(e)}")


# =========================
# EXCEL REPORT BUILDERS
# =========================
def write_df(writer, sheet_name: str, df: pd.DataFrame):
    """Write DataFrame to Excel sheet."""
    sheet_name = sanitize_sheet_name(sheet_name)
    if df is None or df.empty:
        pd.DataFrame([{"message": "No data available"}]).to_excel(
            writer, sheet_name=sheet_name, index=False, startrow=6
        )
    else:
        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=6)


def write_summary_sheet(writer, result: dict):
    """Write executive summary sheet."""
    workbook = writer.book
    ws = workbook.create_sheet("Executive Summary", 0)

    style_report_sheet(
        ws,
        title="Carbon Emissions Report - Executive Summary",
        subtitle="DEFRA 2025 Aligned | Data-Driven Sustainability"
    )

    totals = result.get("totals", {})
    data_quality = result.get("data_quality", {})
    lead_gen = result.get("lead_gen_data", {})
    scope_breakdown = lead_gen.get("breakdown_by_scope", {})
    cat_breakdown = lead_gen.get("breakdown_by_category", {})

    row = 7
    ws[f"A{row}"] = "TOTAL EMISSIONS"
    ws[f"B{row}"] = totals.get("total_tCO2e", 0)
    ws[f"C{row}"] = "tCO2e"
    row += 1

    ws[f"A{row}"] = "Total Emissions (kg)"
    ws[f"B{row}"] = totals.get("total_kgCO2e", 0)
    ws[f"C{row}"] = "kgCO2e"
    row += 1

    ws[f"A{row}"] = "Data Quality Score"
    ws[f"B{row}"] = data_quality.get("coverage_percent", 0)
    ws[f"C{row}"] = "%"
    row += 1

    ws[f"A{row}"] = "Rows Processed"
    ws[f"B{row}"] = data_quality.get("successful_rows", 0)
    ws[f"C{row}"] = f"/ {data_quality.get('total_rows', 0)}"
    row += 2

    ws[f"A{row}"] = "BREAKDOWN BY SCOPE"
    row += 1
    for scope, value in scope_breakdown.items():
        ws[f"A{row}"] = scope
        ws[f"B{row}"] = value
        ws[f"C{row}"] = "tCO2e"
        row += 1

    row += 1
    ws[f"D{row}"] = "BREAKDOWN BY CATEGORY"
    row += 1
    for category, value in cat_breakdown.items():
        ws[f"D{row}"] = category
        ws[f"E{row}"] = value
        ws[f"F{row}"] = "tCO2e"
        row += 1

    auto_fit_columns(ws)


def write_charts_sheet(writer, result: dict):
    """Write charts sheet."""
    workbook = writer.book
    ws = workbook.create_sheet("Charts", 1)

    style_report_sheet(
        ws,
        title="Carbon Emissions Report - Visual Analysis",
        subtitle="Scope and Category Breakdowns"
    )

    line_items = result.get("line_items", [])
    df_results = pd.DataFrame(line_items) if line_items else pd.DataFrame()

    if df_results.empty:
        ws["A7"] = "No data available for charts"
        return

    # Scope breakdown
    if "scope" in df_results.columns:
        df_scope = df_results.groupby("scope")["emissions_kgCO2e"].sum().reset_index()
        ws["A6"] = "Scope"
        ws["B6"] = "kg CO2e"
        for i, row in enumerate(df_scope.itertuples(), start=7):
            ws[f"A{i}"] = row.scope
            ws[f"B{i}"] = float(row.emissions_kgCO2e)

        if len(df_scope) > 0:
            add_bar_chart(ws, "Emissions by Scope", 2, 1, 6, 6 + len(df_scope), "D6")

    # Category breakdown
    if "category" in df_results.columns:
        df_cat = df_results.groupby("category")["emissions_kgCO2e"].sum().reset_index()
        ws["G6"] = "Category"
        ws["H6"] = "kg CO2e"
        for i, row in enumerate(df_cat.itertuples(), start=7):
            ws[f"G{i}"] = row.category
            ws[f"H{i}"] = float(row.emissions_kgCO2e)

        if len(df_cat) > 0:
            add_pie_chart(ws, "Emissions by Category", 8, 7, 6, 6 + len(df_cat), "D24")

    auto_fit_columns(ws)


def build_excel_report(result: dict, uploaded_files_info: list) -> io.BytesIO:
    """Build comprehensive Excel report."""
    output = io.BytesIO()

    df_results = pd.DataFrame(result.get("line_items", []))
    df_errors = pd.DataFrame(result.get("errors", []))
    df_uploaded_files = pd.DataFrame(uploaded_files_info)
    df_fleet_audit = pd.DataFrame([result.get("fleet_audit", {})])
    df_factor_transparency = pd.DataFrame(result.get("factor_transparency", {}).get("factors_used", []))

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        write_summary_sheet(writer, result)
        write_charts_sheet(writer, result)
        
        write_df(writer, "Results", df_results)
        write_df(writer, "Errors", df_errors)
        write_df(writer, "Uploaded Files", df_uploaded_files)
        write_df(writer, "Fleet Audit", df_fleet_audit)
        write_df(writer, "Factor Transparency", df_factor_transparency)

        workbook = writer.book
        for sheet_name in workbook.sheetnames:
            ws = workbook[sheet_name]
            if sheet_name not in ["Executive Summary", "Charts"]:
                style_report_sheet(ws)
            auto_fit_columns(ws)

    output.seek(0)
    return output


# =========================
# NEW API ROUTES - VALIDATION & TEMPLATES
# =========================

@app.post("/api/validate-csv")
async def validate_csv_endpoint(
    file: Annotated[UploadFile, File(description="CSV or Excel file to validate")]
):
    """
    Validate CSV structure before processing.
    
    Returns:
    - Column detection with confidence scores
    - Validation issues and warnings
    - Suggested column mappings
    - Data preview
    """
    try:
        df, file_warnings = read_uploaded_file(file)

        # Run intelligent validation
        validation = CSVValidator.validate_csv(df)
        suggested_mapping = CSVValidator.suggest_column_mapping(df)

        return {
            "file_name": file.filename,
            "rows": len(df),
            "columns": list(df.columns),
            "validation": validation,
            "suggested_mapping": suggested_mapping,
            "preview_rows": df.head(5).to_dict(orient="records"),
            "file_warnings": file_warnings,
            "status": "valid" if validation["is_valid"] else "review_required"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")


@app.get("/api/generate-template")
def generate_template_endpoint(company_name: str = Query("Your Company")):
    """
    Generate a branded CSV template with instructions and examples.
    
    Returns:
    - CSV file download with examples
    - Embedded instructions
    - Supported categories and units
    """
    try:
        template_bytes = generate_csv_template(company_name)

        return StreamingResponse(
            BytesIO(template_bytes),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=carbon_calculator_template_{company_name.replace(' ', '_')}.csv"
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")


@app.get("/api/detect-columns")
async def detect_columns_endpoint(
    file: Annotated[UploadFile, File(description="CSV or Excel file for column detection")]
):
    """
    Intelligently detect and suggest column mappings.
    
    Uses fuzzy matching and pattern recognition to identify:
    - Amount columns (energy, consumption, usage)
    - Unit columns
    - Category columns
    - Date/period columns
    - Vehicle type columns
    """
    try:
        df, _ = read_uploaded_file(file)

        # Run intelligent column detection
        column_detection = IntelligentColumnDetector.auto_detect_columns(df)

        return {
            "file_name": file.filename,
            "total_columns": len(df.columns),
            "column_detection": {
                col: {
                    "intent": det["intent"].value,
                    "confidence": det["confidence"],
                    "extracted_unit": det["extracted_unit"],
                    "sample_values": det["sample_values"]
                }
                for col, det in column_detection.items()
            },
            "recommendations": CSVValidator.suggest_column_mapping(df),
            "preview": df.head(3).to_dict(orient="records")
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")


@app.get("/api/factor-audit-log")
def factor_audit_endpoint(year: int = Query(DEFAULT_FACTOR_YEAR)):
    """
    Get complete DEFRA factor audit log for a specific year.
    
    Returns all supported factors including:
    - Electricity factors
    - Fuel factors
    - Vehicle factors
    - Water factors
    - Waste factors
    """
    try:
        audit_log = get_factor_audit_log(year)
        return {
            "audit_year": year,
            "generated_at": datetime.now().isoformat(),
            "factors": audit_log,
            "supported_years": get_supported_years()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")


@app.get("/api/supported-years")
def supported_years_endpoint():
    """Get list of supported DEFRA years."""
    try:
        years = get_supported_years()
        return {
            "supported_years": years,
            "default_year": DEFAULT_FACTOR_YEAR,
            "latest_year": max(years)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")


# =========================
# MAIN CALCULATION ROUTES
# =========================

@app.post("/api/calculate")
def calculate_direct(payload: dict):
    """
    Direct calculation endpoint.
    
    Accepts JSON payload with activities array.
    Use for programmatic integrations.
    """
    try:
        activities = payload.get("activities", [])
        company_name = payload.get("company_name", "")
        reporting_period = payload.get("reporting_period", "")

        if not isinstance(activities, list):
            raise HTTPException(status_code=400, detail="activities must be a list")

        if not activities:
            raise HTTPException(status_code=400, detail="activities list cannot be empty")

        # Process emissions
        batch_result = calculate_emissions_batch_safe(activities)

        # Build response
        response = build_api_response(
            batch_result=batch_result,
            uploaded_files_info=[],
            company_name=company_name,
            reporting_period=reporting_period,
            user={}
        )

        return make_json_safe(response)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")


@app.post("/api/upload-calculate")
async def upload_calculate(
    files: Annotated[List[UploadFile], File(description="Upload one or more CSV/XLSX activity datasets")],
    privacy_consent: Annotated[str, Form(...)],
    contact_consent: Annotated[str, Form(...)],
    full_name: Annotated[str, Form(...)],
    email: Annotated[str, Form(...)],
    company_name: Annotated[str, Form(...)],
    phone_number: Annotated[str, Form(...)],
    reporting_period: Annotated[str, Form(default="")],
):
    """
    Upload CSV/Excel and get JSON response.
    
    Includes:
    - Automatic column detection
    - Data validation
    - Factor transparency
    - Lead-gen formatted data
    - Fleet audit logs
    """
    try:
        # Validate user inputs
        validate_user_inputs(
            privacy_consent=privacy_consent,
            contact_consent=contact_consent,
            full_name=full_name,
            email=email,
            company_name=company_name,
            phone_number=phone_number,
        )

        user = build_user_object(full_name, email, company_name, phone_number)

        # Prepare combined dataframe with intelligent mapping
        combined_df_clean, uploaded_files_info, file_level_errors = prepare_combined_dataframe(files)
        activities = combined_df_clean.to_dict(orient="records")

        # Run emissions engine
        batch_result = calculate_emissions_batch_safe(activities)

        # Merge file errors
        batch_result = merge_file_errors_with_result(batch_result, file_level_errors)

        # Build API response
        api_result = build_api_response(
            batch_result=batch_result,
            uploaded_files_info=uploaded_files_info,
            company_name=company_name,
            reporting_period=reporting_period,
            user=user,
            privacy_consent=True,
            contact_consent=True
        )

        # Send lead data to external logger
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

        return make_json_safe(api_result)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")


@app.post("/api/upload-calculate-download")
async def upload_calculate_download(
    files: Annotated[List[UploadFile], File(description="Upload one or more CSV/XLSX activity datasets")],
    privacy_consent: Annotated[str, Form(...)],
    contact_consent: Annotated[str, Form(...)],
    full_name: Annotated[str, Form(...)],
    email: Annotated[str, Form(...)],
    company_name: Annotated[str, Form(...)],
    phone_number: Annotated[str, Form(...)],
    reporting_period: Annotated[str, Form(default="")],
):
    """
    Upload CSV/Excel and download Excel report.
    
    Includes:
    - Executive summary sheet
    - Visual charts
    - Detailed line items
    - Error logs
    - Factor transparency
    - Fleet audit
    """
    try:
        # Validate user inputs
        validate_user_inputs(
            privacy_consent=privacy_consent,
            contact_consent=contact_consent,
            full_name=full_name,
            email=email,
            company_name=company_name,
            phone_number=phone_number,
        )

        user = build_user_object(full_name, email, company_name, phone_number)

        # Prepare combined dataframe with intelligent mapping
        combined_df_clean, uploaded_files_info, file_level_errors = prepare_combined_dataframe(files)
        activities = combined_df_clean.to_dict(orient="records")

        # Run emissions engine
        batch_result = calculate_emissions_batch_safe(activities)

        # Merge file errors
        batch_result = merge_file_errors_with_result(batch_result, file_level_errors)

        # Build API response
        api_result = build_api_response(
            batch_result=batch_result,
            uploaded_files_info=uploaded_files_info,
            company_name=company_name,
            reporting_period=reporting_period,
            user=user,
            privacy_consent=True,
            contact_consent=True
        )

        # Build Excel report
        excel_output = build_excel_report(api_result, uploaded_files_info)

        # Send lead data to external logger
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

        return StreamingResponse(
            excel_output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=carbon_emissions_report_{company_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.xlsx"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")


# =========================
# BACKWARDS COMPATIBILITY ROUTES
# =========================

@app.post("/calculate")
def calculate_legacy(payload: dict):
    """Legacy route - redirects to new endpoint."""
    return calculate_direct(payload)


@app.post("/upload-calculate")
async def upload_calculate_legacy(
    files: Annotated[List[UploadFile], File(description="Upload one or more CSV/XLSX activity datasets")],
    privacy_consent: Annotated[str, Form(...)],
    contact_consent: Annotated[str, Form(...)],
    full_name: Annotated[str, Form(...)],
    email: Annotated[str, Form(...)],
    company_name: Annotated[str, Form(...)],
    phone_number: Annotated[str, Form(...)],
):
    """Legacy route - redirects to new endpoint."""
    return await upload_calculate(
        files, privacy_consent, contact_consent, full_name, email, company_name, phone_number, ""
    )


@app.post("/upload-calculate-download")
async def upload_calculate_download_legacy(
    files: Annotated[List[UploadFile], File(description="Upload one or more CSV/XLSX activity datasets")],
    privacy_consent: Annotated[str, Form(...)],
    contact_consent: Annotated[str, Form(...)],
    full_name: Annotated[str, Form(...)],
    email: Annotated[str, Form(...)],
    company_name: Annotated[str, Form(...)],
    phone_number: Annotated[str, Form(...)],
):
    """Legacy route - redirects to new endpoint."""
    return await upload_calculate_download(
        files, privacy_consent, contact_consent, full_name, email, company_name, phone_number, ""
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

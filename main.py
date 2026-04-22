from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import List, Annotated, Dict, Any, Tuple
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

from engine import run_emissions_engine

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


DEFAULT_PARSER_OPTIONS = {
    "include_fleet_fuel": True,
    "include_business_mileage": False,
    "include_business_mileage_when_fuel_present": False,
    "include_ev_charging_submeter": False,
    "fleet_fuel_type": "Diesel",
    "electricity_country": "Electricity: UK",
}


def build_parser_options(form_values: Dict[str, Any] | None = None) -> Dict[str, Any]:
    options = dict(DEFAULT_PARSER_OPTIONS)
    if not form_values:
        return options

    for key, value in form_values.items():
        if value is None or value == "":
            continue
        if key in {
            "include_fleet_fuel",
            "include_business_mileage",
            "include_business_mileage_when_fuel_present",
            "include_ev_charging_submeter",
        }:
            options[key] = to_bool_string_true(value)
        else:
            options[key] = value
    return options


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

    if ws.max_row >= 7:
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


# =========================
# VALIDATION / USER
# =========================
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
        "phone_number": phone_number,
    }


# =========================
# SOURCE PREPARATION
# =========================
def dataframe_to_workbook_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    df = df.copy()
    df = df.where(pd.notnull(df), None)
    records: List[Dict[str, Any]] = []
    for row in df.itertuples(index=False, name=None):
        record = {f"col_{idx + 1}": value for idx, value in enumerate(row)}
        records.append(record)
    return records


def extract_file_sources(uploaded_file: UploadFile) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    filename = uploaded_file.filename or "uploaded_file"
    filename_lower = filename.lower()
    content = uploaded_file.file.read()

    uploaded_sources: List[Dict[str, Any]] = []
    file_level_errors: List[Dict[str, Any]] = []

    try:
        if filename_lower.endswith(".csv"):
            df = pd.read_csv(
                BytesIO(content),
                dtype=str,
                keep_default_na=False,
                engine="python",
                on_bad_lines="skip",
            )
            df = normalize_uploaded_columns(df)
            uploaded_sources.append(
                {
                    "source_file": filename,
                    "source_type": "csv",
                    "sheet_name": None,
                    "records": df.where(pd.notnull(df), None).to_dict(orient="records"),
                    "rows": int(len(df)),
                    "columns": [str(c) for c in df.columns],
                }
            )
            return uploaded_sources, file_level_errors

        if filename_lower.endswith(".xlsx") or filename_lower.endswith(".xls"):
            excel_file = pd.ExcelFile(BytesIO(content))
            if not excel_file.sheet_names:
                file_level_errors.append({
                    "source_file": filename,
                    "error": "Workbook contained no readable sheets.",
                })
                return uploaded_sources, file_level_errors

            for sheet_name in excel_file.sheet_names:
                try:
                    df_sheet = pd.read_excel(excel_file, sheet_name=sheet_name, header=None, dtype=object)
                    raw_rows = int(df_sheet.shape[0])
                    raw_cols = int(df_sheet.shape[1])
                    records = dataframe_to_workbook_records(df_sheet)
                    uploaded_sources.append(
                        {
                            "source_file": filename,
                            "source_type": "excel_sheet",
                            "sheet_name": sheet_name,
                            "records": records,
                            "rows": raw_rows,
                            "columns": [f"col_{i + 1}" for i in range(raw_cols)],
                        }
                    )
                except Exception as sheet_error:
                    file_level_errors.append({
                        "source_file": filename,
                        "source_sheet": sheet_name,
                        "error": f"Could not read sheet '{sheet_name}': {type(sheet_error).__name__}: {str(sheet_error)}",
                    })
            return uploaded_sources, file_level_errors

        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file format for '{filename}'. Please upload CSV or Excel files.",
        )

    except HTTPException:
        raise
    except Exception as e:
        file_level_errors.append({
            "source_file": filename,
            "error": f"Unexpected file read error: {type(e).__name__}: {str(e)}",
        })
        return uploaded_sources, file_level_errors


def prepare_uploaded_sources(files: List[UploadFile]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded.")

    all_sources: List[Dict[str, Any]] = []
    file_level_errors: List[Dict[str, Any]] = []
    input_row_count = 0

    for uploaded_file in files:
        sources, errors = extract_file_sources(uploaded_file)
        all_sources.extend(sources)
        file_level_errors.extend(errors)
        input_row_count += sum(int(src.get("rows", 0) or 0) for src in sources)

    if not all_sources and not file_level_errors:
        raise HTTPException(status_code=400, detail="No readable files were uploaded.")

    return all_sources, file_level_errors, input_row_count


# =========================
# ENGINE ORCHESTRATION
# =========================
def _sum_numeric(values: List[Any]) -> float:
    total = 0.0
    for value in values:
        try:
            total += float(value or 0)
        except Exception:
            pass
    return round(total, 4)


def _merge_count_dicts(dicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    for item in dicts:
        key = safe_string(item.get("category") or item.get("error") or item.get("factor_quality") or item.get("title") or item.get("metric"))
        if not key:
            continue
        counts[key] = counts.get(key, 0) + int(item.get("count", 0) or 0)
    key_name = None
    for candidate in ["category", "error", "factor_quality", "title", "metric"]:
        if dicts and candidate in dicts[0]:
            key_name = candidate
            break
    if not key_name:
        key_name = "label"
    return [{key_name: key, "count": value} for key, value in counts.items()]


def _combine_issue_groups(issue_groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for item in issue_groups:
        title = safe_string(item.get("title"))
        if not title:
            continue
        existing = grouped.get(title)
        if not existing:
            grouped[title] = {
                "title": title,
                "count": int(item.get("count", 0) or 0),
                "severity": item.get("severity", "low"),
                "guidance": item.get("guidance", ""),
                "examples": list(item.get("examples", []) or []),
            }
        else:
            existing["count"] += int(item.get("count", 0) or 0)
            existing["examples"] = (existing["examples"] + list(item.get("examples", []) or []))[:5]
            severity_rank = {"high": 3, "medium": 2, "low": 1}
            if severity_rank.get(item.get("severity", "low"), 0) > severity_rank.get(existing.get("severity", "low"), 0):
                existing["severity"] = item.get("severity", "low")
    severity_order = {"high": 0, "medium": 1, "low": 2}
    return sorted(grouped.values(), key=lambda x: (severity_order.get(x.get("severity", "low"), 9), -x.get("count", 0), x.get("title", "")))


def combine_engine_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    line_items: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    factor_transparency: List[Dict[str, Any]] = []
    uploaded_scope_summary: List[Dict[str, Any]] = []
    uploaded_scope_category_summary: List[Dict[str, Any]] = []
    parser_warnings: List[Dict[str, Any]] = []
    parser_diagnostics: List[Dict[str, Any]] = []
    top_categories: List[Dict[str, Any]] = []
    top_sites: List[Dict[str, Any]] = []
    issue_groups: List[Dict[str, Any]] = []
    unmapped_categories: List[Dict[str, Any]] = []
    factor_quality_summary: List[Dict[str, Any]] = []

    totals_kg_values: List[Any] = []
    input_rows = 0
    successful_rows = 0
    errored_rows = 0
    mapped_rows = 0
    inferred_rows = 0
    exact_rows = 0
    estimated_rows = 0
    official_rows = 0
    starter_rows = 0
    extracted_activities = 0

    for result in results:
        line_items.extend(result.get("line_items", []))
        errors.extend(result.get("errors", []))
        factor_transparency.extend(result.get("factor_transparency", []))
        uploaded_scope_summary.extend(result.get("summary_by_scope", []))
        uploaded_scope_category_summary.extend(result.get("summary_by_scope_category", []))
        top_categories.extend(result.get("top_categories_by_kgco2e", []))
        top_sites.extend(result.get("top_sites_by_kgco2e", []))
        issue_groups.extend(result.get("issue_groups", []))
        unmapped_categories.extend(result.get("unmapped_categories", []))
        factor_quality_summary.extend(result.get("factor_quality_summary", []))
        totals_kg_values.append((result.get("totals") or {}).get("total_kgCO2e", 0))

        dq = result.get("data_quality", {}) or {}
        input_rows += int(dq.get("total_rows", 0) or 0)
        successful_rows += int(dq.get("successful_rows", 0) or 0)
        errored_rows += int(dq.get("errored_rows", 0) or 0)
        mapped_rows += int(dq.get("mapped_category_rows", 0) or 0)
        inferred_rows += int(dq.get("inferred_category_rows", 0) or 0)
        exact_rows += int(dq.get("exact_factor_rows", 0) or 0)
        estimated_rows += int(dq.get("estimated_rows", 0) or 0)
        official_rows += int(dq.get("official_factor_rows", 0) or 0)
        starter_rows += int(dq.get("starter_factor_rows", 0) or 0)

        pdx = result.get("parser_diagnostics", {}) or {}
        extracted_activities += int(pdx.get("extracted_activity_count", 0) or 0)
        if pdx:
            parser_diagnostics.append(pdx)
            for warning in pdx.get("warnings", []) or []:
                parser_warnings.append({
                    "source_file": pdx.get("source_file", ""),
                    "source_sheet": pdx.get("source_sheet", ""),
                    "error": warning,
                })

    df_line = pd.DataFrame(line_items)
    if not df_line.empty and "emissions_kgCO2e" in df_line.columns:
        df_line["emissions_kgCO2e"] = pd.to_numeric(df_line["emissions_kgCO2e"], errors="coerce").fillna(0)
        df_line["emissions_tCO2e"] = df_line["emissions_kgCO2e"] / 1000

        df_scope = pd.DataFrame()
        if "scope" in df_line.columns:
            df_scope = (
                df_line.groupby("scope", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
                .sum()
                .sort_values("emissions_kgCO2e", ascending=False)
                .reset_index(drop=True)
            )
            total_scope_kg = float(df_scope["emissions_kgCO2e"].sum()) if not df_scope.empty else 0.0
            df_scope["percent_of_total"] = df_scope["emissions_kgCO2e"].apply(
                lambda x: round((float(x) / total_scope_kg) * 100, 2) if total_scope_kg > 0 else 0.0
            )
        else:
            df_scope = pd.DataFrame(columns=["scope", "emissions_kgCO2e", "emissions_tCO2e", "percent_of_total"])

        df_scope_cat = pd.DataFrame()
        if {"scope", "category"}.issubset(df_line.columns):
            df_scope_cat = (
                df_line.groupby(["scope", "category"], as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
                .sum()
                .sort_values("emissions_kgCO2e", ascending=False)
                .reset_index(drop=True)
            )
        else:
            df_scope_cat = pd.DataFrame(columns=["scope", "category", "emissions_kgCO2e", "emissions_tCO2e"])

        df_cat = pd.DataFrame()
        if "category" in df_line.columns:
            df_cat = (
                df_line.groupby("category", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
                .sum()
                .sort_values("emissions_kgCO2e", ascending=False)
                .head(5)
                .reset_index(drop=True)
            )
        else:
            df_cat = pd.DataFrame(columns=["category", "emissions_kgCO2e", "emissions_tCO2e"])

        df_sites = pd.DataFrame()
        if "site_name" in df_line.columns:
            working = df_line.copy()
            working["site_name"] = working["site_name"].fillna("Unspecified site")
            df_sites = (
                working.groupby("site_name", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
                .sum()
                .sort_values("emissions_kgCO2e", ascending=False)
                .head(5)
                .reset_index(drop=True)
            )
        else:
            df_sites = pd.DataFrame(columns=["site_name", "emissions_kgCO2e", "emissions_tCO2e"])
    else:
        df_scope = pd.DataFrame(columns=["scope", "emissions_kgCO2e", "emissions_tCO2e", "percent_of_total"])
        df_scope_cat = pd.DataFrame(columns=["scope", "category", "emissions_kgCO2e", "emissions_tCO2e"])
        df_cat = pd.DataFrame(columns=["category", "emissions_kgCO2e", "emissions_tCO2e"])
        df_sites = pd.DataFrame(columns=["site_name", "emissions_kgCO2e", "emissions_tCO2e"])

    total_kg = _sum_numeric(totals_kg_values)
    total_t = round(total_kg / 1000, 4)
    coverage_percent = round((successful_rows / input_rows) * 100, 2) if input_rows > 0 else 0.0

    confidence_score = 0
    if input_rows > 0:
        confidence_score = round(
            max(
                0.0,
                min(
                    100.0,
                    100.0
                    - max(0.0, 100.0 - coverage_percent) * 0.6
                    - (estimated_rows / max(input_rows, 1)) * 15.0
                    - (starter_rows / max(input_rows, 1)) * 15.0,
                ),
            ),
            0,
        )
    confidence_label = "High" if confidence_score >= 80 else "Moderate" if confidence_score >= 60 else "Low"

    notes: List[str] = []
    if extracted_activities == 0:
        notes.append("No calculable activities were extracted from the uploaded files.")
    if coverage_percent > 0:
        notes.append(f"{coverage_percent}% of extracted activity rows were processed successfully.")
    if starter_rows > 0:
        notes.append(f"{starter_rows} row(s) used starter placeholder factor tables.")
    if not notes:
        notes.append("Review parser diagnostics and errors before relying on this result externally.")

    analytics = {}
    if not df_line.empty and "emissions_kgCO2e" in df_line.columns:
        analytics["average_emissions_per_row_kgco2e"] = round(float(df_line["emissions_kgCO2e"].mean()), 4)
        if not df_cat.empty:
            analytics["top_category_by_kgco2e"] = df_cat.iloc[0].to_dict()
            analytics["top_categories_by_kgco2e"] = df_cat.to_dict(orient="records")
        if not df_sites.empty:
            analytics["top_site_by_kgco2e"] = df_sites.iloc[0].to_dict()
            analytics["top_sites_by_kgco2e"] = df_sites.to_dict(orient="records")
        if not df_scope.empty:
            analytics["top_scope_by_kgco2e"] = df_scope.iloc[0].to_dict()
    else:
        analytics["average_emissions_per_row_kgco2e"] = 0.0

    plain_language_takeaways = []
    if total_kg > 0:
        plain_language_takeaways.append(f"Total reported emissions are {round(total_t, 2)} tCO2e ({round(total_kg, 2)} kgCO2e).")
    if not df_scope.empty:
        row0 = df_scope.iloc[0]
        plain_language_takeaways.append(f"{row0['scope']} is the largest contributor at {round(float(row0['percent_of_total']), 2)}% of total emissions.")
    if extracted_activities > 0:
        plain_language_takeaways.append(f"The parser extracted {extracted_activities} calculable activity row(s) from the uploaded files.")
    if coverage_percent < 100:
        plain_language_takeaways.append(f"Coverage is {coverage_percent}%, so some extracted rows were excluded due to validation or factor-matching issues.")
    if not plain_language_takeaways:
        plain_language_takeaways.append("No reportable emissions were produced from the uploaded data.")

    actionable_insights = []
    if extracted_activities == 0:
        actionable_insights.append({
            "title": "Parser review required",
            "body": "No calculable activity rows were extracted. Check workbook structure, source labels, and parser diagnostics before issuing a client report.",
        })
    if coverage_percent < 90 and input_rows > 0:
        actionable_insights.append({
            "title": "Coverage improvement needed",
            "body": f"Coverage is currently {coverage_percent}%. Resolve parser warnings and validation errors before relying on totals externally.",
        })
    if starter_rows > 0:
        actionable_insights.append({
            "title": "Replace starter factors",
            "body": f"{starter_rows} row(s) used starter placeholder factors. Replace these with your official DEFRA-aligned factor library for external reporting.",
        })

    return {
        "totals": {"total_kgCO2e": total_kg, "total_tCO2e": total_t},
        "summary_by_scope": df_scope.to_dict(orient="records"),
        "summary_by_scope_category": df_scope_cat.to_dict(orient="records"),
        "line_items": line_items,
        "analytics": analytics,
        "top_categories_by_kgco2e": df_cat.to_dict(orient="records"),
        "top_sites_by_kgco2e": df_sites.to_dict(orient="records"),
        "plain_language_takeaways": plain_language_takeaways[:6],
        "actionable_insights": actionable_insights[:8],
        "confidence_score": {
            "score": int(confidence_score),
            "label": confidence_label,
            "coverage_percent": coverage_percent,
            "notes": notes,
        },
        "data_quality": {
            "total_rows": input_rows,
            "successful_rows": successful_rows,
            "errored_rows": errored_rows,
            "coverage_percent": coverage_percent,
            "mapped_category_rows": mapped_rows,
            "inferred_category_rows": inferred_rows,
            "exact_factor_rows": exact_rows,
            "estimated_rows": estimated_rows,
            "official_factor_rows": official_rows,
            "starter_factor_rows": starter_rows,
            "valid_rows_percent": coverage_percent,
            "invalid_rows_percent": round((errored_rows / input_rows) * 100, 2) if input_rows > 0 else 0.0,
            "extracted_activity_count": extracted_activities,
        },
        "parser_diagnostics": parser_diagnostics,
        "errors": parser_warnings + errors,
        "errors_preview": (parser_warnings + errors)[:50],
        "error_summary": pd.DataFrame(parser_warnings + errors)["error"].astype(str).value_counts().reset_index().rename(columns={"index": "error", "error": "count"}).to_dict(orient="records") if (parser_warnings + errors) else [],
        "issue_groups": _combine_issue_groups(issue_groups),
        "unmapped_categories": _merge_count_dicts(unmapped_categories),
        "factor_transparency": factor_transparency,
        "factor_quality_summary": _merge_count_dicts(factor_quality_summary),
        "methodology_summary": {
            "framework": "GHG Protocol aligned reporting structure using uploaded activity data and emissions factors.",
            "factor_basis": "Workbook-style uploads are parsed into normalized activities before calculation. Fuel, electricity, gas, and water use built-in factors in the current engine. Some business-travel and waste paths remain starter tables until replaced with your official DEFRA source tables.",
            "calculation_logic": "Each uploaded source is parsed, normalized into activity rows, matched to an emissions factor, multiplied by the activity quantity, and aggregated into total CO2e outputs.",
            "assumptions": [
                "Fleet fuel litres are included by default.",
                "Business mileage is excluded by default when direct fleet fuel data is present to avoid double counting.",
                "EV charging sub-meter rows are excluded by default to avoid double counting against main electricity unless explicitly enabled.",
            ],
            "interpretation_notes": [
                "Review parser diagnostics and excluded sections alongside the final totals.",
                "Do not issue a client report if extracted activity count is zero or coverage is materially incomplete.",
            ],
        },
    }


def run_engine_for_uploaded_sources(
    uploaded_sources: List[Dict[str, Any]],
    parser_options: Dict[str, Any],
) -> Dict[str, Any]:
    source_results: List[Dict[str, Any]] = []

    for source in uploaded_sources:
        request_json = {
            "activities": source.get("records", []),
            "parser_options": {
                **parser_options,
                "source_file": source.get("source_file"),
                "source_sheet": source.get("sheet_name"),
                "source_workbook": source.get("source_file"),
            },
        }
        result = run_emissions_engine(request_json)
        pdx = result.get("parser_diagnostics", {}) or {}
        pdx["source_file"] = source.get("source_file")
        pdx["source_sheet"] = source.get("sheet_name")
        result["parser_diagnostics"] = pdx
        source_results.append(result)

    return combine_engine_results(source_results)


# =========================
# RESULT ENRICHMENT
# =========================
def merge_file_errors_with_result(base_result: dict, file_level_errors: list) -> dict:
    result = dict(base_result)
    combined_errors = file_level_errors + result.get("errors", [])
    result["errors"] = combined_errors
    result["errors_preview"] = combined_errors[:50]

    if combined_errors:
        df_errors = pd.DataFrame(combined_errors)
        if "error" in df_errors.columns:
            grouped = df_errors["error"].astype(str).value_counts().reset_index()
            grouped.columns = ["error", "count"]
            result["error_summary"] = grouped.to_dict(orient="records")
    else:
        result["error_summary"] = []

    return result


def build_api_response(
    full_result: dict,
    uploaded_files_info: list,
    input_row_count: int,
    user: dict,
    parser_options: dict,
    privacy_consent: bool = True,
    contact_consent: bool = True,
) -> dict:
    errors = full_result.get("errors", [])
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
        "parser_options": parser_options,
        "analytics": full_result.get("analytics", {}),
        "top_categories_by_kgco2e": full_result.get("top_categories_by_kgco2e", []),
        "top_sites_by_kgco2e": full_result.get("top_sites_by_kgco2e", []),
        "plain_language_takeaways": full_result.get("plain_language_takeaways", []),
        "actionable_insights": full_result.get("actionable_insights", []),
        "confidence_score": full_result.get("confidence_score", {}),
        "methodology_summary": full_result.get("methodology_summary", {}),
        "factor_quality_summary": full_result.get("factor_quality_summary", []),
        "error_count": len(errors),
        "errors": errors,
        "errors_preview": full_result.get("errors_preview", errors[:50]),
        "error_summary": full_result.get("error_summary", []),
        "issue_groups": full_result.get("issue_groups", []),
        "data_quality": full_result.get("data_quality", {}),
        "unmapped_categories": full_result.get("unmapped_categories", []),
        "factor_transparency": full_result.get("factor_transparency", []),
        "parser_diagnostics": full_result.get("parser_diagnostics", []),
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
    data_quality = result.get("data_quality", {})

    payload = {
        "full_name": user_data.get("full_name", ""),
        "email": user_data.get("email", ""),
        "company_name": user_data.get("company_name", ""),
        "phone_number": user_data.get("phone_number", ""),
        "privacy_consent": user_data.get("privacy_consent", False),
        "contact_consent": user_data.get("contact_consent", False),
        "uploaded_files_count": len(uploaded_files_info),
        "input_row_count": result.get("input_row_count", 0),
        "extracted_activity_count": data_quality.get("extracted_activity_count", 0),
        "total_kgco2e": totals.get("total_kgCO2e", 0),
        "total_tco2e": totals.get("total_tCO2e", 0),
        "top_category": (analytics.get("top_category_by_kgco2e") or {}).get("category", ""),
        "top_site": (analytics.get("top_site_by_kgco2e") or {}).get("site_name", ""),
        "confidence_score": confidence.get("score", 0),
        "source": "website_calculator",
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
        "extracted_activity_count": dq.get("extracted_activity_count", 0),
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
        "extracted_activity_count": dq.get("extracted_activity_count", 0),
    }])


def write_summary_sheet(writer, result: dict):
    workbook = writer.book
    ws = workbook.create_sheet("Executive Summary")

    style_report_sheet(
        ws,
        title="GS Carbon Emissions Report",
        subtitle="Executive summary for uploaded datasets",
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
    ws["A12"] = "Extracted activity count"
    ws["B12"] = dq.get("extracted_activity_count", 0)
    ws["A13"] = "Error count"
    ws["B13"] = result.get("error_count", 0)

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

    ws["A16"] = "Scope"
    ws["B16"] = "kg CO2e"
    ws["C16"] = "% of total"
    for i, row in enumerate(scope_rows, start=17):
        ws[f"A{i}"] = row.get("scope")
        ws[f"B{i}"] = row.get("emissions_kgCO2e")
        ws[f"C{i}"] = row.get("percent_of_total")

    ws["E16"] = "Top category"
    ws["F16"] = "kg CO2e"
    for i, row in enumerate(top_categories, start=17):
        ws[f"E{i}"] = row.get("category")
        ws[f"F{i}"] = row.get("emissions_kgCO2e")

    ws["G16"] = "Issue group"
    ws["H16"] = "Severity"
    ws["I16"] = "Count"
    ws["J16"] = "Guidance"
    for i, row in enumerate(issue_groups, start=17):
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
        subtitle="Visual summary of uploaded datasets",
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
    df_parser_diagnostics = pd.DataFrame(result.get("parser_diagnostics", []))
    df_parser_options = pd.DataFrame([result.get("parser_options", {})]) if result.get("parser_options") else pd.DataFrame()

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
        write_df(writer, "Parser Diagnostics", df_parser_diagnostics)
        write_df(writer, "Parser Options", df_parser_options)
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
        parser_options = build_parser_options(payload.get("parser_options", {}))

        if not isinstance(activities, list):
            raise HTTPException(status_code=400, detail="activities must be a list")

        return run_emissions_engine({"activities": activities, "parser_options": parser_options})

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
    include_fleet_fuel: Annotated[str, Form("true")],
    include_business_mileage: Annotated[str, Form("false")],
    include_business_mileage_when_fuel_present: Annotated[str, Form("false")],
    include_ev_charging_submeter: Annotated[str, Form("false")],
    fleet_fuel_type: Annotated[str, Form("Diesel")],
    electricity_country: Annotated[str, Form("Electricity: UK")],
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
        parser_options = build_parser_options({
            "include_fleet_fuel": include_fleet_fuel,
            "include_business_mileage": include_business_mileage,
            "include_business_mileage_when_fuel_present": include_business_mileage_when_fuel_present,
            "include_ev_charging_submeter": include_ev_charging_submeter,
            "fleet_fuel_type": fleet_fuel_type,
            "electricity_country": electricity_country,
        })

        uploaded_sources, file_level_errors, input_row_count = prepare_uploaded_sources(files)
        uploaded_files_info = [
            {
                "filename": src.get("source_file"),
                "sheet_name": src.get("sheet_name"),
                "source_type": src.get("source_type"),
                "rows": src.get("rows", 0),
                "columns": src.get("columns", []),
            }
            for src in uploaded_sources
        ]

        full_result = run_engine_for_uploaded_sources(uploaded_sources, parser_options)
        full_result = merge_file_errors_with_result(full_result, file_level_errors)

        api_result = build_api_response(
            full_result=full_result,
            uploaded_files_info=uploaded_files_info,
            input_row_count=input_row_count,
            user=user,
            parser_options=parser_options,
            privacy_consent=True,
            contact_consent=True,
        )

        send_lead_to_logger(
            {
                "full_name": full_name,
                "email": email,
                "company_name": company_name,
                "phone_number": phone_number,
                "privacy_consent": True,
                "contact_consent": True,
            },
            api_result,
            uploaded_files_info,
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
    include_fleet_fuel: Annotated[str, Form("true")],
    include_business_mileage: Annotated[str, Form("false")],
    include_business_mileage_when_fuel_present: Annotated[str, Form("false")],
    include_ev_charging_submeter: Annotated[str, Form("false")],
    fleet_fuel_type: Annotated[str, Form("Diesel")],
    electricity_country: Annotated[str, Form("Electricity: UK")],
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
        parser_options = build_parser_options({
            "include_fleet_fuel": include_fleet_fuel,
            "include_business_mileage": include_business_mileage,
            "include_business_mileage_when_fuel_present": include_business_mileage_when_fuel_present,
            "include_ev_charging_submeter": include_ev_charging_submeter,
            "fleet_fuel_type": fleet_fuel_type,
            "electricity_country": electricity_country,
        })

        uploaded_sources, file_level_errors, input_row_count = prepare_uploaded_sources(files)
        uploaded_files_info = [
            {
                "filename": src.get("source_file"),
                "sheet_name": src.get("sheet_name"),
                "source_type": src.get("source_type"),
                "rows": src.get("rows", 0),
                "columns": src.get("columns", []),
            }
            for src in uploaded_sources
        ]

        full_result = run_engine_for_uploaded_sources(uploaded_sources, parser_options)
        full_result = merge_file_errors_with_result(full_result, file_level_errors)

        api_result = build_api_response(
            full_result=full_result,
            uploaded_files_info=uploaded_files_info,
            input_row_count=input_row_count,
            user=user,
            parser_options=parser_options,
            privacy_consent=True,
            contact_consent=True,
        )

        output = build_excel_report(api_result, uploaded_files_info)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=carbon_emissions_report.xlsx"},
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")

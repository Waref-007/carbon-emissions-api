import pandas as pd
import numpy as np


# =========================
# FACTOR TABLES
# =========================
# These are the cleaned DEFRA 2025 factors currently supported by the engine.
# Later, you can replace this with loading from saved CSV/JSON files.

df_fuels_clean = pd.DataFrame([
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "kWh (Gross CV)", "kg CO2e": 0.18296},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "kWh (Net CV)", "kg CO2e": 0.20270},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "cubic metres", "kg CO2e": 2.06672},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "tonnes", "kg CO2e": 2575.46441},
])

df_electricity_clean = pd.DataFrame([
    {"Activity": "Electricity generated", "Country": "Electricity: UK", "Unit": "kWh", "Year": 2025, "kg CO2e": 0.177}
])

df_water_clean = pd.DataFrame([
    {"Activity": "Water supply", "Type": "Water supply", "Unit": "cubic metres", "kg CO2e": 0.1913},
    {"Activity": "Water supply", "Type": "Water supply", "Unit": "million litres", "kg CO2e": 191.30156},
])


# =========================
# NORMALIZATION DICTIONARIES
# =========================
FUEL_ALIASES = {
    "natural gas": "Natural gas",
    "gas": "Natural gas",
}

UNIT_ALIASES = {
    "kwh (gross cv)": "kWh (Gross CV)",
    "gross cv": "kWh (Gross CV)",
    "gross": "kWh (Gross CV)",
    "kwh gross": "kWh (Gross CV)",
    "kwh": "kWh (Gross CV)",
    "kwh (net cv)": "kWh (Net CV)",
    "net cv": "kWh (Net CV)",
    "net": "kWh (Net CV)",
    "litre": "litres",
    "litres": "litres",
    "l": "litres",
    "cubic metre": "cubic metres",
    "cubic metres": "cubic metres",
    "m3": "cubic metres",
    "tonne": "tonnes",
    "tonnes": "tonnes",
    "kwh ": "kWh (Gross CV)",
    " kwh ": "kWh (Gross CV)",
    "kwh": "kWh (Gross CV)",
    "kwh gross cv": "kWh (Gross CV)",
    "kwh gross": "kWh (Gross CV)",
    "kwh (gross cv)": "kWh (Gross CV)",
}

WATER_TYPE_ALIASES = {
    "water supply": "Water supply"
}

WATER_UNIT_ALIASES = {
    "cubic metre": "cubic metres",
    "cubic metres": "cubic metres",
    "m3": "cubic metres",
    "million litres": "million litres"
}


# =========================
# VALIDATION HELPERS
# =========================
def validate_text_input(value, field_name):
    if value is None:
        raise ValueError(f"{field_name} is required.")
    value = str(value).strip()
    if value == "" or value.lower() == "nan":
        raise ValueError(f"{field_name} is required.")
    return value


def validate_amount(amount):
    if amount is None:
        raise ValueError("amount is required.")
    try:
        amount = float(amount)
    except Exception:
        raise ValueError("amount must be numeric.")
    if amount < 0:
        raise ValueError("amount cannot be negative.")
    return amount


def validate_year(year):
    if year is None:
        raise ValueError("year is required.")
    try:
        year = int(float(year))
    except Exception:
        raise ValueError("year must be an integer.")
    if year < 1900 or year > 2100:
        raise ValueError("year is out of valid range.")
    return year


# =========================
# NORMALIZATION HELPERS
# =========================
def normalize_fuel(fuel):
    key = str(fuel).strip().lower()
    return FUEL_ALIASES.get(key, fuel)


def normalize_unit(unit):
    key = str(unit).strip().lower()
    return UNIT_ALIASES.get(key, unit)


def normalize_water_type(water_type):
    key = str(water_type).strip().lower()
    return WATER_TYPE_ALIASES.get(key, water_type)


def normalize_water_unit(unit):
    key = str(unit).strip().lower()
    return WATER_UNIT_ALIASES.get(key, unit)


# =========================
# OUTPUT HELPER
# =========================
def build_result_row(
    category,
    sub_category,
    item_name,
    scope,
    input_amount,
    input_unit,
    normalized_unit,
    emission_factor,
    factor_year,
    emissions_kg,
    data_source="DEFRA 2025"
):
    return pd.DataFrame([{
        "category": category,
        "sub_category": sub_category,
        "item_name": item_name,
        "scope": scope,
        "input_amount": input_amount,
        "input_unit": input_unit,
        "normalized_unit": normalized_unit,
        "emission_factor_kgCO2e_per_unit": emission_factor,
        "factor_year": factor_year,
        "emissions_kgCO2e": emissions_kg,
        "emissions_tCO2e": emissions_kg / 1000,
        "data_source": data_source
    }])


# =========================
# CALCULATORS
# =========================
def calculate_fuel_emissions_unified(df, fuel, unit, amount, factor_year=2025):
    fuel = validate_text_input(fuel, "fuel")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)

    fuel_normalized = normalize_fuel(fuel)
    unit_normalized = normalize_unit(unit)

    match = df[
        (df["Fuel"].str.strip().str.lower() == fuel_normalized.strip().lower()) &
        (df["Unit"].str.strip().str.lower() == unit_normalized.strip().lower())
    ].copy()

    if match.empty:
        raise ValueError(
            f"No matching factor found for Fuel='{fuel}' "
            f"(normalized='{fuel_normalized}') and Unit='{unit}' "
            f"(normalized='{unit_normalized}')"
        )

    row = match.iloc[0]
    factor = float(row["kg CO2e"])
    emissions_kg = amount * factor

    return build_result_row(
        category="Fuel",
        sub_category=row["Activity"],
        item_name=row["Fuel"],
        scope="Scope 1",
        input_amount=amount,
        input_unit=unit,
        normalized_unit=row["Unit"],
        emission_factor=factor,
        factor_year=factor_year,
        emissions_kg=emissions_kg,
    )


def calculate_electricity_emissions_unified(df, country, unit, amount, year=2025):
    country = validate_text_input(country, "country")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)
    year = validate_year(year)

    match = df[
        (df["Country"].str.strip().str.lower() == country.strip().lower()) &
        (df["Unit"].str.strip().str.lower() == unit.strip().lower()) &
        (df["Year"] == year)
    ].copy()

    if match.empty:
        raise ValueError(
            f"No matching electricity factor found for Country='{country}', Unit='{unit}', Year={year}"
        )

    row = match.iloc[0]
    factor = float(row["kg CO2e"])
    emissions_kg = amount * factor

    return build_result_row(
        category="Electricity",
        sub_category=row["Activity"],
        item_name=row["Country"],
        scope="Scope 2",
        input_amount=amount,
        input_unit=unit,
        normalized_unit=row["Unit"],
        emission_factor=factor,
        factor_year=int(row["Year"]),
        emissions_kg=emissions_kg,
    )


def calculate_water_emissions_unified(df, water_type, unit, amount, factor_year=2025):
    water_type = validate_text_input(water_type, "water_type")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)

    water_type_normalized = normalize_water_type(water_type)
    unit_normalized = normalize_water_unit(unit)

    match = df[
        (df["Type"].str.strip().str.lower() == water_type_normalized.strip().lower()) &
        (df["Unit"].str.strip().str.lower() == unit_normalized.strip().lower())
    ].copy()

    if match.empty:
        raise ValueError(
            f"No matching water factor found for Type='{water_type}' "
            f"(normalized='{water_type_normalized}') and Unit='{unit}' "
            f"(normalized='{unit_normalized}')"
        )

    row = match.iloc[0]
    factor = float(row["kg CO2e"])
    emissions_kg = amount * factor

    return build_result_row(
        category="Water",
        sub_category=row["Activity"],
        item_name=row["Type"],
        scope="Scope 3",
        input_amount=amount,
        input_unit=unit,
        normalized_unit=row["Unit"],
        emission_factor=factor,
        factor_year=factor_year,
        emissions_kg=emissions_kg,
    )


# =========================
# PREPROCESSING
# =========================
def preprocess_uploaded_dataframe(df):
    df_clean = df.copy()

    object_cols = df_clean.select_dtypes(include=["object"]).columns
    for col in object_cols:
        df_clean[col] = df_clean[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    df_clean = df_clean.replace(r"^\s*$", None, regex=True)

    if "category" in df_clean.columns:
        df_clean["category"] = df_clean["category"].apply(
            lambda x: x.lower() if isinstance(x, str) else x
        )

    df_clean = df_clean.where(pd.notnull(df_clean), None)
    return df_clean


# =========================
# DISPATCHER
# =========================
def calculate_emissions(category, **kwargs):
    category_clean = validate_text_input(category, "category").strip().lower()

    if category_clean == "fuel":
        return calculate_fuel_emissions_unified(
            df=df_fuels_clean,
            fuel=kwargs.get("fuel"),
            unit=kwargs.get("unit"),
            amount=kwargs.get("amount"),
            factor_year=kwargs.get("factor_year", 2025)
        )

    elif category_clean == "electricity":
        return calculate_electricity_emissions_unified(
            df=df_electricity_clean,
            country=kwargs.get("country"),
            unit=kwargs.get("unit"),
            amount=kwargs.get("amount"),
            year=kwargs.get("year", 2025)
        )

    elif category_clean == "water":
        return calculate_water_emissions_unified(
            df=df_water_clean,
            water_type=kwargs.get("water_type"),
            unit=kwargs.get("unit"),
            amount=kwargs.get("amount"),
            factor_year=kwargs.get("factor_year", 2025)
        )

    else:
        raise ValueError(
            f"Unsupported category: '{category}'. Supported categories are: fuel, electricity, water."
        )


# =========================
# BATCH PROCESSING
# =========================
METADATA_FIELDS = ["record_id", "client_name", "site_name", "reporting_period", "notes"]

def calculate_emissions_batch_safe(activity_list):
    results = []
    errors = []

    for i, activity in enumerate(activity_list):
        try:
            activity_data = activity.copy()
            category = activity_data.pop("category", None)

            metadata = {field: activity.get(field, None) for field in METADATA_FIELDS}

            result = calculate_emissions(category=category, **activity_data)
            result = result.copy()
            result["row_index"] = i

            for field, value in metadata.items():
                result[field] = value

            results.append(result)

        except Exception as e:
            errors.append({
                "row_index": i,
                "input": activity,
                "error": str(e)
            })

    if results:
        final_report = pd.concat(results, ignore_index=True)
        total_kg = final_report["emissions_kgCO2e"].sum()
        total_t = final_report["emissions_tCO2e"].sum()
    else:
        final_report = pd.DataFrame()
        total_kg = 0.0
        total_t = 0.0

    return {
        "report": final_report,
        "total_kgCO2e": total_kg,
        "total_tCO2e": total_t,
        "errors": errors
    }


# =========================
# SUMMARIES
# =========================
def summarize_report_by_scope_and_category(report_df):
    if report_df.empty:
        return pd.DataFrame(columns=["scope", "category", "emissions_kgCO2e", "emissions_tCO2e"])

    return (
        report_df
        .groupby(["scope", "category"], as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
        .sum()
        .sort_values(["scope", "category"])
        .reset_index(drop=True)
    )


def summarize_report_by_scope(report_df):
    if report_df.empty:
        return pd.DataFrame(columns=["scope", "emissions_kgCO2e", "emissions_tCO2e"])

    return (
        report_df
        .groupby("scope", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
        .sum()
        .sort_values("scope")
        .reset_index(drop=True)
    )


# =========================
# RESPONSE BUILDERS
# =========================
def build_final_response(batch_result):
    report_df = batch_result["report"]
    summary_scope_category = summarize_report_by_scope_and_category(report_df)
    summary_scope = summarize_report_by_scope(report_df)

    return {
        "line_items": report_df.to_dict(orient="records"),
        "summary_by_scope_category": summary_scope_category.to_dict(orient="records"),
        "summary_by_scope": summary_scope.to_dict(orient="records"),
        "totals": {
            "total_kgCO2e": batch_result["total_kgCO2e"],
            "total_tCO2e": batch_result["total_tCO2e"]
        },
        "errors": batch_result["errors"]
    }


def make_json_safe(obj):
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return obj


def run_emissions_engine(request_json):
    activities = request_json.get("activities", [])

    # If called with uploaded tabular data converted to records, preprocessing is already done.
    # If needed later for file uploads, you can preprocess before this stage.

    batch_result = calculate_emissions_batch_safe(activities)
    final_response = build_final_response(batch_result)
    final_response_json_safe = make_json_safe(final_response)

    return final_response_json_safe

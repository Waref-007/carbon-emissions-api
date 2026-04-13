import pandas as pd
import numpy as np


# =========================
# FACTOR TABLES
# =========================
# Keep your exact known factors for fuel / electricity / water.
# Business travel and waste remain starter placeholder factors until
# replaced with your official DEFRA 2025 source tables.

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

df_business_travel_clean = pd.DataFrame([
    {"Activity": "Business travel", "Travel Type": "Hotel stay", "Unit": "room_nights", "kg CO2e": 10.40, "Scope": "Scope 3"},
    {"Activity": "Business travel", "Travel Type": "Hotel stay", "Unit": "gbp", "kg CO2e": 0.12, "Scope": "Scope 3"},
    {"Activity": "Business travel", "Travel Type": "Flight", "Unit": "passenger_km", "kg CO2e": 0.146, "Scope": "Scope 3"},
    {"Activity": "Business travel", "Travel Type": "Rail", "Unit": "passenger_km", "kg CO2e": 0.035, "Scope": "Scope 3"},
    {"Activity": "Business travel", "Travel Type": "Taxi / car", "Unit": "vehicle_km", "kg CO2e": 0.170, "Scope": "Scope 3"},
    {"Activity": "Business travel", "Travel Type": "Generic travel spend", "Unit": "gbp", "kg CO2e": 0.28, "Scope": "Scope 3"},
])

df_waste_clean = pd.DataFrame([
    {"Activity": "Waste generated", "Waste Type": "General waste", "Unit": "tonnes", "kg CO2e": 250.0, "Scope": "Scope 3"},
    {"Activity": "Waste generated", "Waste Type": "Landfill waste", "Unit": "tonnes", "kg CO2e": 458.0, "Scope": "Scope 3"},
    {"Activity": "Waste generated", "Waste Type": "Recycling", "Unit": "tonnes", "kg CO2e": 21.0, "Scope": "Scope 3"},
    {"Activity": "Waste generated", "Waste Type": "Incineration", "Unit": "tonnes", "kg CO2e": 27.0, "Scope": "Scope 3"},
    {"Activity": "Waste generated", "Waste Type": "General waste", "Unit": "kg", "kg CO2e": 0.250, "Scope": "Scope 3"},
    {"Activity": "Waste generated", "Waste Type": "Landfill waste", "Unit": "kg", "kg CO2e": 0.458, "Scope": "Scope 3"},
    {"Activity": "Waste generated", "Waste Type": "Recycling", "Unit": "kg", "kg CO2e": 0.021, "Scope": "Scope 3"},
    {"Activity": "Waste generated", "Waste Type": "Incineration", "Unit": "kg", "kg CO2e": 0.027, "Scope": "Scope 3"},
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
    "kwh ": "kWh (Gross CV)",
    " kwh ": "kWh (Gross CV)",
    "kwh gross cv": "kWh (Gross CV)",
    "kwh gross": "kWh (Gross CV)",

    "kwh (net cv)": "kWh (Net CV)",
    "net cv": "kWh (Net CV)",
    "net": "kWh (Net CV)",

    "kwh": "kWh",

    "litre": "litres",
    "litres": "litres",
    "l": "litres",

    "cubic metre": "cubic metres",
    "cubic metres": "cubic metres",
    "m3": "cubic metres",

    "tonne": "tonnes",
    "tonnes": "tonnes",

    "room night": "room_nights",
    "room nights": "room_nights",
    "night": "room_nights",
    "nights": "room_nights",

    "gbp": "gbp",
    "£": "gbp",
    "spend": "gbp",

    "km": "vehicle_km",
    "vehicle km": "vehicle_km",
    "vehicle_km": "vehicle_km",

    "passenger km": "passenger_km",
    "passenger_km": "passenger_km",

    "kg": "kg",
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

CATEGORY_ALIASES = {
    "fuel": "fuel",
    "gas": "fuel",
    "natural gas": "fuel",

    "electricity": "electricity",
    "power": "electricity",

    "water": "water",
    "water supply": "water",

    "travel": "business travel",
    "business travel": "business travel",
    "hotel": "business travel",
    "accommodation": "business travel",
    "flight": "business travel",
    "rail": "business travel",
    "train": "business travel",
    "taxi": "business travel",
    "car travel": "business travel",

    "waste": "waste",
    "general waste": "waste",
    "landfill": "waste",
    "recycling": "waste",
    "incineration": "waste",
}

TRAVEL_TYPE_ALIASES = {
    "hotel": "Hotel stay",
    "hotel stay": "Hotel stay",
    "accommodation": "Hotel stay",

    "travel": "Generic travel spend",
    "business travel": "Generic travel spend",

    "flight": "Flight",
    "air travel": "Flight",
    "plane": "Flight",

    "rail": "Rail",
    "train": "Rail",

    "taxi": "Taxi / car",
    "car": "Taxi / car",
    "car travel": "Taxi / car",
    "vehicle": "Taxi / car",
}

WASTE_TYPE_ALIASES = {
    "waste": "General waste",
    "general waste": "General waste",
    "landfill": "Landfill waste",
    "landfill waste": "Landfill waste",
    "recycling": "Recycling",
    "incineration": "Incineration",
}


# =========================
# CONSTANTS
# =========================
DEFAULT_FACTOR_YEAR = 2025
SUPPORTED_CATEGORY_HINT = "Supported / mappable categories include: fuel, electricity, water, travel, hotel, waste."

METADATA_FIELDS = [
    "record_id",
    "client_name",
    "site_name",
    "reporting_period",
    "notes",
    "source_file",
    "row_index_original"
]


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
        if isinstance(amount, str):
            amount = amount.replace(",", "").strip()
        amount = float(amount)
    except Exception:
        raise ValueError("amount must be numeric.")
    if amount < 0:
        raise ValueError("amount cannot be negative.")
    return amount


def validate_year(year):
    if year is None:
        return DEFAULT_FACTOR_YEAR
    try:
        year = int(float(year))
    except Exception:
        raise ValueError("year must be an integer.")
    if year < 1900 or year > 2100:
        raise ValueError("year is out of valid range.")
    return year


def safe_text(value):
    if value is None:
        return None
    value = str(value).strip()
    if value == "" or value.lower() == "nan":
        return None
    return value


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return float(value)
    except Exception:
        return default


def is_blank(value):
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if str(value).strip() == "":
        return True
    if str(value).strip().lower() == "nan":
        return True
    return False


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


def normalize_category(category):
    key = str(category).strip().lower()
    return CATEGORY_ALIASES.get(key, None)


def normalize_travel_type(value):
    if value is None:
        return None
    key = str(value).strip().lower()
    return TRAVEL_TYPE_ALIASES.get(key, None)


def normalize_waste_type(value):
    if value is None:
        return None
    key = str(value).strip().lower()
    return WASTE_TYPE_ALIASES.get(key, None)


# =========================
# INFERENCE HELPERS
# =========================
def infer_canonical_category(activity):
    raw_category = safe_text(activity.get("category"))
    raw_item_name = safe_text(activity.get("item_name"))
    raw_sub_category = safe_text(activity.get("sub_category"))
    raw_notes = safe_text(activity.get("notes"))

    candidates = [raw_category, raw_item_name, raw_sub_category, raw_notes]

    for value in candidates:
        if not value:
            continue
        mapped = normalize_category(value)
        if mapped:
            confidence = "high" if value == raw_category else "medium"
            return {
                "raw_category": raw_category,
                "canonical_category": mapped,
                "mapping_source": "category" if value == raw_category else "inferred",
                "mapping_confidence": confidence
            }

    return {
        "raw_category": raw_category,
        "canonical_category": None,
        "mapping_source": "unmapped",
        "mapping_confidence": "low"
    }


def infer_business_travel_type(activity):
    raw_category = safe_text(activity.get("category"))
    item_name = safe_text(activity.get("item_name"))
    sub_category = safe_text(activity.get("sub_category"))
    notes = safe_text(activity.get("notes"))

    candidates = [raw_category, item_name, sub_category, notes]

    for value in candidates:
        mapped = normalize_travel_type(value)
        if mapped:
            return mapped

    unit = normalize_unit(activity.get("unit")) if activity.get("unit") is not None else None
    if unit == "room_nights":
        return "Hotel stay"
    if unit == "passenger_km":
        return "Flight"
    if unit == "vehicle_km":
        return "Taxi / car"
    if unit == "gbp":
        return "Generic travel spend"

    return "Generic travel spend"


def infer_waste_type(activity):
    raw_category = safe_text(activity.get("category"))
    item_name = safe_text(activity.get("item_name"))
    sub_category = safe_text(activity.get("sub_category"))
    notes = safe_text(activity.get("notes"))

    candidates = [raw_category, item_name, sub_category, notes]

    for value in candidates:
        mapped = normalize_waste_type(value)
        if mapped:
            return mapped

    return "General waste"


def classify_calculation_basis(mapping_source, factor_quality):
    if factor_quality == "official" and mapping_source == "category":
        return "exact"
    if factor_quality == "official" and mapping_source == "inferred":
        return "estimated"
    if factor_quality != "official":
        return "estimated"
    return "estimated"


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
    data_source="DEFRA 2025",
    factor_name=None,
    factor_description=None,
    factor_quality="official",
    original_category=None,
    mapping_source=None,
    mapping_confidence=None
):
    calculation_basis = classify_calculation_basis(mapping_source, factor_quality)

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
        "data_source": data_source,
        "factor_name": factor_name or item_name,
        "factor_description": factor_description or "",
        "factor_quality": factor_quality,
        "original_category": original_category,
        "mapping_source": mapping_source,
        "mapping_confidence": mapping_confidence,
        "calculation_basis": calculation_basis
    }])


# =========================
# CALCULATORS
# =========================
def calculate_fuel_emissions_unified(
    df,
    fuel,
    unit,
    amount,
    factor_year=DEFAULT_FACTOR_YEAR,
    original_category=None,
    mapping_source=None,
    mapping_confidence=None
):
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
        data_source="DEFRA 2025",
        factor_name=f"{row['Fuel']} / {row['Unit']}",
        factor_description="Fuel combustion factor",
        factor_quality="official",
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence
    )


def calculate_electricity_emissions_unified(
    df,
    country,
    unit,
    amount,
    year=DEFAULT_FACTOR_YEAR,
    original_category=None,
    mapping_source=None,
    mapping_confidence=None
):
    country = validate_text_input(country, "country")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)
    year = validate_year(year)

    unit_normalized = normalize_unit(unit)

    match = df[
        (df["Country"].str.strip().str.lower() == country.strip().lower()) &
        (df["Unit"].str.strip().str.lower() == unit_normalized.strip().lower()) &
        (df["Year"] == year)
    ].copy()

    if match.empty:
        raise ValueError(
            f"No matching electricity factor found for Country='{country}', Unit='{unit_normalized}', Year={year}"
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
        data_source="DEFRA 2025",
        factor_name=f"{row['Country']} / {row['Unit']}",
        factor_description="Location-based electricity factor",
        factor_quality="official",
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence
    )


def calculate_water_emissions_unified(
    df,
    water_type,
    unit,
    amount,
    factor_year=DEFAULT_FACTOR_YEAR,
    original_category=None,
    mapping_source=None,
    mapping_confidence=None
):
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
        data_source="DEFRA 2025",
        factor_name=f"{row['Type']} / {row['Unit']}",
        factor_description="Water supply factor",
        factor_quality="official",
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence
    )


def calculate_business_travel_emissions_unified(
    df,
    travel_type,
    unit,
    amount,
    factor_year=DEFAULT_FACTOR_YEAR,
    original_category=None,
    mapping_source=None,
    mapping_confidence=None
):
    travel_type = validate_text_input(travel_type, "travel_type")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)

    unit_normalized = normalize_unit(unit)

    match = df[
        (df["Travel Type"].str.strip().str.lower() == travel_type.strip().lower()) &
        (df["Unit"].str.strip().str.lower() == unit_normalized.strip().lower())
    ].copy()

    if match.empty:
        raise ValueError(
            f"No matching business travel factor found for Travel Type='{travel_type}' and Unit='{unit_normalized}'. "
            f"Expected example units include: room_nights, passenger_km, vehicle_km, gbp."
        )

    row = match.iloc[0]
    factor = float(row["kg CO2e"])
    emissions_kg = amount * factor

    return build_result_row(
        category="Business Travel",
        sub_category=row["Activity"],
        item_name=row["Travel Type"],
        scope=row["Scope"],
        input_amount=amount,
        input_unit=unit,
        normalized_unit=row["Unit"],
        emission_factor=factor,
        factor_year=factor_year,
        emissions_kg=emissions_kg,
        data_source="Starter Scope 3 factor library - replace with your official DEFRA 2025 table",
        factor_name=f"{row['Travel Type']} / {row['Unit']}",
        factor_description="Starter business travel factor",
        factor_quality="starter_placeholder",
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence
    )


def calculate_waste_emissions_unified(
    df,
    waste_type,
    unit,
    amount,
    factor_year=DEFAULT_FACTOR_YEAR,
    original_category=None,
    mapping_source=None,
    mapping_confidence=None
):
    waste_type = validate_text_input(waste_type, "waste_type")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)

    unit_normalized = normalize_unit(unit)

    match = df[
        (df["Waste Type"].str.strip().str.lower() == waste_type.strip().lower()) &
        (df["Unit"].str.strip().str.lower() == unit_normalized.strip().lower())
    ].copy()

    if match.empty:
        raise ValueError(
            f"No matching waste factor found for Waste Type='{waste_type}' and Unit='{unit_normalized}'. "
            f"Expected example units include: tonnes, kg."
        )

    row = match.iloc[0]
    factor = float(row["kg CO2e"])
    emissions_kg = amount * factor

    return build_result_row(
        category="Waste",
        sub_category=row["Activity"],
        item_name=row["Waste Type"],
        scope=row["Scope"],
        input_amount=amount,
        input_unit=unit,
        normalized_unit=row["Unit"],
        emission_factor=factor,
        factor_year=factor_year,
        emissions_kg=emissions_kg,
        data_source="Starter Scope 3 factor library - replace with your official DEFRA 2025 table",
        factor_name=f"{row['Waste Type']} / {row['Unit']}",
        factor_description="Starter waste factor",
        factor_quality="starter_placeholder",
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence
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

    if "unit" in df_clean.columns:
        df_clean["unit"] = df_clean["unit"].apply(
            lambda x: normalize_unit(x) if isinstance(x, str) else x
        )

    if "fuel" in df_clean.columns:
        df_clean["fuel"] = df_clean["fuel"].apply(
            lambda x: normalize_fuel(x) if isinstance(x, str) else x
        )

    if "water_type" in df_clean.columns:
        df_clean["water_type"] = df_clean["water_type"].apply(
            lambda x: normalize_water_type(x) if isinstance(x, str) else x
        )

    df_clean = df_clean.where(pd.notnull(df_clean), None)
    return df_clean


# =========================
# DISPATCHER
# =========================
def calculate_emissions(activity):
    mapping = infer_canonical_category(activity)

    original_category = mapping["raw_category"]
    canonical_category = mapping["canonical_category"]
    mapping_source = mapping["mapping_source"]
    mapping_confidence = mapping["mapping_confidence"]

    if not canonical_category:
        raise ValueError(
            f"Unsupported or unmapped category: '{original_category}'. {SUPPORTED_CATEGORY_HINT}"
        )

    unit = activity.get("unit")
    amount = activity.get("amount")

    if canonical_category == "fuel":
        fuel = activity.get("fuel") or activity.get("item_name") or activity.get("sub_category") or "Natural gas"
        return calculate_fuel_emissions_unified(
            df=df_fuels_clean,
            fuel=fuel,
            unit=unit,
            amount=amount,
            factor_year=activity.get("factor_year", DEFAULT_FACTOR_YEAR),
            original_category=original_category,
            mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    if canonical_category == "electricity":
        country = activity.get("country") or activity.get("item_name") or "Electricity: UK"
        unit = unit or "kWh"
        return calculate_electricity_emissions_unified(
            df=df_electricity_clean,
            country=country,
            unit=unit,
            amount=amount,
            year=activity.get("year", DEFAULT_FACTOR_YEAR),
            original_category=original_category,
            mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    if canonical_category == "water":
        water_type = activity.get("water_type") or activity.get("item_name") or "Water supply"
        return calculate_water_emissions_unified(
            df=df_water_clean,
            water_type=water_type,
            unit=unit,
            amount=amount,
            factor_year=activity.get("factor_year", DEFAULT_FACTOR_YEAR),
            original_category=original_category,
            mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    if canonical_category == "business travel":
        travel_type = infer_business_travel_type(activity)
        return calculate_business_travel_emissions_unified(
            df=df_business_travel_clean,
            travel_type=travel_type,
            unit=unit,
            amount=amount,
            factor_year=activity.get("factor_year", DEFAULT_FACTOR_YEAR),
            original_category=original_category,
            mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    if canonical_category == "waste":
        waste_type = infer_waste_type(activity)
        return calculate_waste_emissions_unified(
            df=df_waste_clean,
            waste_type=waste_type,
            unit=unit,
            amount=amount,
            factor_year=activity.get("factor_year", DEFAULT_FACTOR_YEAR),
            original_category=original_category,
            mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    raise ValueError(
        f"Unsupported category: '{original_category}'. {SUPPORTED_CATEGORY_HINT}"
    )


# =========================
# ERROR GROUPING / QUALITY
# =========================
def infer_issue_type_from_message(message):
    msg = str(message or "").lower()

    if "unmapped category" in msg or "unsupported category" in msg:
        return "Unmapped category"
    if "amount is required" in msg:
        return "Missing amount"
    if "must be numeric" in msg:
        return "Non-numeric amount"
    if "cannot be negative" in msg:
        return "Negative amount"
    if "unit is required" in msg:
        return "Missing unit"
    if "no matching factor found" in msg or "no matching electricity factor found" in msg or "no matching water factor found" in msg:
        return "Factor matching failure"
    if "year must be" in msg or "year is out of valid range" in msg:
        return "Invalid year"
    if "required" in msg:
        return "Missing required field"
    return "Other validation issue"


def infer_severity(issue_type, count=1):
    high_types = {
        "Unmapped category",
        "Factor matching failure",
        "Missing required field",
        "Missing unit",
        "Missing amount",
        "Non-numeric amount",
    }
    medium_types = {
        "Invalid year",
        "Negative amount",
        "Other validation issue",
    }

    if issue_type in high_types:
        return "high"
    if issue_type in medium_types:
        return "medium"
    if count >= 10:
        return "high"
    if count >= 3:
        return "medium"
    return "low"


def infer_issue_guidance(issue_type):
    guidance_map = {
        "Unmapped category": "Standardise the category labels in the source data so they match the calculator taxonomy, or extend the alias mapping table.",
        "Missing amount": "Add a valid numeric amount for the affected rows before re-uploading.",
        "Non-numeric amount": "Replace text or malformed values in the amount field with clean numeric values.",
        "Negative amount": "Check whether the input quantity should be positive. If credits or reversals are intended, handle them explicitly.",
        "Missing unit": "Populate the missing unit and ensure it matches the supported unit list for that category.",
        "Factor matching failure": "Review the combination of category, subtype, country, fuel, travel type, and unit so the correct emission factor can be matched.",
        "Invalid year": "Use a valid integer year within the supported range.",
        "Missing required field": "Complete the missing required columns or fields before submitting the dataset.",
        "Other validation issue": "Review the affected rows shown in the preview and correct the source data before re-uploading."
    }
    return guidance_map.get(issue_type, "Review the affected rows and correct the source data before re-uploading.")


def group_errors(errors):
    if not errors:
        return []

    grouped = {}

    for err in errors:
        issue_type = infer_issue_type_from_message(err.get("error"))
        if issue_type not in grouped:
            grouped[issue_type] = {
                "title": issue_type,
                "count": 0,
                "severity": "low",
                "guidance": infer_issue_guidance(issue_type),
                "examples": []
            }

        grouped[issue_type]["count"] += 1

        example_bits = []
        if err.get("source_file"):
            example_bits.append(f"[{err['source_file']}]")
        if err.get("row_index_original") is not None:
            example_bits.append(f"row {err['row_index_original']}")
        elif err.get("row_index") is not None:
            example_bits.append(f"row {err['row_index']}")
        if err.get("error"):
            example_bits.append(err["error"])

        example_text = " - ".join(example_bits).strip()
        if example_text and len(grouped[issue_type]["examples"]) < 5:
            grouped[issue_type]["examples"].append(example_text)

    for issue_type, item in grouped.items():
        item["severity"] = infer_severity(issue_type, item["count"])

    grouped_list = list(grouped.values())
    severity_order = {"high": 0, "medium": 1, "low": 2}
    grouped_list.sort(key=lambda x: (severity_order.get(x["severity"], 9), -x["count"], x["title"]))

    return grouped_list


def build_error_summary(errors):
    grouped = group_errors(errors)
    return [{"error": item["title"], "count": item["count"]} for item in grouped]


def build_errors_preview(errors, max_items=10):
    return errors[:max_items]


def calculate_confidence_score(report_df, data_quality):
    total_rows = int(data_quality.get("total_rows", 0) or 0)
    coverage_percent = float(data_quality.get("coverage_percent", 0.0) or 0.0)
    inferred_rows = int(data_quality.get("inferred_category_rows", 0) or 0)
    estimated_rows = int(data_quality.get("estimated_rows", 0) or 0)
    placeholder_rows = int(data_quality.get("starter_factor_rows", 0) or 0)

    if total_rows <= 0:
        return {
            "score": 0,
            "label": "Low",
            "coverage_percent": 0.0,
            "notes": ["No valid rows were available for calculation."]
        }

    score = 100.0

    score -= max(0.0, 100.0 - coverage_percent) * 0.45
    score -= (inferred_rows / total_rows) * 15.0
    score -= (estimated_rows / total_rows) * 20.0
    score -= (placeholder_rows / total_rows) * 20.0

    score = round(max(0.0, min(100.0, score)), 0)

    if score >= 80:
        label = "High"
    elif score >= 60:
        label = "Moderate"
    else:
        label = "Low"

    notes = []
    if coverage_percent >= 95:
        notes.append("Most rows were processed successfully.")
    elif coverage_percent > 0:
        notes.append("Some rows were excluded due to validation or factor-matching issues.")

    if inferred_rows > 0:
        notes.append(f"{inferred_rows} row(s) relied on inferred category mapping.")
    if estimated_rows > 0:
        notes.append(f"{estimated_rows} row(s) used estimated or non-exact treatment.")
    if placeholder_rows > 0:
        notes.append(f"{placeholder_rows} row(s) used starter placeholder factor tables that should be replaced with your official DEFRA dataset.")

    if not notes:
        notes.append("No material data quality limitations were detected in the returned result set.")

    return {
        "score": int(score),
        "label": label,
        "coverage_percent": coverage_percent,
        "notes": notes
    }


# =========================
# BATCH PROCESSING
# =========================
def calculate_emissions_batch_safe(activity_list):
    results = []
    errors = []
    category_mapping_rows = []
    unmapped_categories = []

    for i, activity in enumerate(activity_list):
        try:
            activity_data = activity.copy()

            metadata = {field: activity.get(field, None) for field in METADATA_FIELDS}
            mapping = infer_canonical_category(activity_data)

            category_mapping_rows.append({
                "row_index": i,
                "original_category": mapping.get("raw_category"),
                "canonical_category": mapping.get("canonical_category"),
                "mapping_source": mapping.get("mapping_source"),
                "mapping_confidence": mapping.get("mapping_confidence")
            })

            if not mapping.get("canonical_category"):
                unmapped_categories.append(mapping.get("raw_category"))
                raise ValueError(
                    f"Unsupported or unmapped category: '{mapping.get('raw_category')}'. {SUPPORTED_CATEGORY_HINT}"
                )

            result = calculate_emissions(activity_data)
            result = result.copy()
            result["row_index"] = i

            for field, value in metadata.items():
                result[field] = value

            results.append(result)

        except Exception as e:
            errors.append({
                "row_index": i,
                "row_index_original": activity.get("row_index_original", i + 1),
                "source_file": activity.get("source_file"),
                "original_category": activity.get("category"),
                "input": activity,
                "error": str(e)
            })

    if results:
        final_report = pd.concat(results, ignore_index=True)
        total_kg = float(final_report["emissions_kgCO2e"].sum())
        total_t = float(final_report["emissions_tCO2e"].sum())
    else:
        final_report = pd.DataFrame()
        total_kg = 0.0
        total_t = 0.0

    total_rows = len(activity_list)
    successful_rows = len(final_report)
    errored_rows = len(errors)
    coverage_percent = round((successful_rows / total_rows) * 100, 2) if total_rows > 0 else 0.0

    mapping_df = pd.DataFrame(category_mapping_rows)
    if not mapping_df.empty:
        mapped_rows = int(mapping_df["canonical_category"].notna().sum())
        inferred_rows = int((mapping_df["mapping_source"] == "inferred").sum())
    else:
        mapped_rows = 0
        inferred_rows = 0

    unmapped_summary = []
    if unmapped_categories:
        unmapped_df = pd.Series(unmapped_categories, dtype="object").fillna("blank").value_counts().reset_index()
        unmapped_df.columns = ["category", "count"]
        unmapped_summary = unmapped_df.to_dict(orient="records")

    exact_factor_rows = 0
    estimated_rows = 0
    starter_factor_rows = 0
    official_factor_rows = 0

    if not final_report.empty:
        exact_factor_rows = int((final_report["calculation_basis"] == "exact").sum()) if "calculation_basis" in final_report.columns else 0
        estimated_rows = int((final_report["calculation_basis"] == "estimated").sum()) if "calculation_basis" in final_report.columns else 0
        starter_factor_rows = int((final_report["factor_quality"] == "starter_placeholder").sum()) if "factor_quality" in final_report.columns else 0
        official_factor_rows = int((final_report["factor_quality"] == "official").sum()) if "factor_quality" in final_report.columns else 0

    data_quality = {
        "total_rows": total_rows,
        "successful_rows": successful_rows,
        "errored_rows": errored_rows,
        "coverage_percent": coverage_percent,
        "mapped_category_rows": mapped_rows,
        "inferred_category_rows": inferred_rows,
        "exact_factor_rows": exact_factor_rows,
        "estimated_rows": estimated_rows,
        "official_factor_rows": official_factor_rows,
        "starter_factor_rows": starter_factor_rows,
        "valid_rows_percent": round((successful_rows / total_rows) * 100, 2) if total_rows > 0 else 0.0,
        "invalid_rows_percent": round((errored_rows / total_rows) * 100, 2) if total_rows > 0 else 0.0
    }

    confidence = calculate_confidence_score(final_report, data_quality)

    return {
        "report": final_report,
        "total_kgCO2e": total_kg,
        "total_tCO2e": total_t,
        "errors": errors,
        "data_quality": data_quality,
        "unmapped_categories": unmapped_summary,
        "confidence": confidence
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
        return pd.DataFrame(columns=["scope", "emissions_kgCO2e", "emissions_tCO2e", "percent_of_total"])

    summary = (
        report_df
        .groupby("scope", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
        .sum()
        .sort_values("emissions_kgCO2e", ascending=False)
        .reset_index(drop=True)
    )

    total_kg = float(summary["emissions_kgCO2e"].sum()) if not summary.empty else 0.0
    summary["percent_of_total"] = summary["emissions_kgCO2e"].apply(
        lambda x: round((float(x) / total_kg) * 100, 2) if total_kg > 0 else 0.0
    )
    return summary


def summarize_top_categories(report_df, top_n=5):
    if report_df.empty:
        return []

    df = (
        report_df
        .groupby("category", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
        .sum()
        .sort_values("emissions_kgCO2e", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    return df.to_dict(orient="records")


def summarize_top_sites(report_df, top_n=5):
    if report_df.empty or "site_name" not in report_df.columns:
        return []

    working = report_df.copy()
    working["site_name"] = working["site_name"].fillna("Unspecified site")

    df = (
        working
        .groupby("site_name", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
        .sum()
        .sort_values("emissions_kgCO2e", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    return df.to_dict(orient="records")


def summarize_factor_quality(report_df):
    if report_df.empty or "factor_quality" not in report_df.columns:
        return []

    df = report_df["factor_quality"].fillna("unknown").value_counts().reset_index()
    df.columns = ["factor_quality", "count"]
    return df.to_dict(orient="records")


def build_factor_transparency(report_df):
    if report_df.empty:
        return []

    cols = [
        "category",
        "scope",
        "factor_name",
        "factor_description",
        "emission_factor_kgCO2e_per_unit",
        "normalized_unit",
        "factor_year",
        "data_source",
        "factor_quality",
        "calculation_basis"
    ]

    available_cols = [c for c in cols if c in report_df.columns]
    factor_df = report_df[available_cols].drop_duplicates().reset_index(drop=True)

    factor_df = factor_df.sort_values(
        by=[c for c in ["category", "factor_name"] if c in factor_df.columns]
    ).reset_index(drop=True)

    return factor_df.to_dict(orient="records")


# =========================
# ANALYTICS / INSIGHTS
# =========================
def build_analytics(report_df):
    analytics = {}

    if report_df.empty:
        analytics["average_emissions_per_row_kgco2e"] = 0.0
        return analytics

    analytics["average_emissions_per_row_kgco2e"] = round(float(report_df["emissions_kgCO2e"].mean()), 4)

    category_df = (
        report_df
        .groupby("category", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
        .sum()
        .sort_values("emissions_kgCO2e", ascending=False)
        .reset_index(drop=True)
    )
    if not category_df.empty:
        analytics["top_category_by_kgco2e"] = category_df.iloc[0].to_dict()
        analytics["top_categories_by_kgco2e"] = category_df.head(5).to_dict(orient="records")

    if "site_name" in report_df.columns:
        site_df = (
            report_df.assign(site_name=report_df["site_name"].fillna("Unspecified site"))
            .groupby("site_name", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
            .sum()
            .sort_values("emissions_kgCO2e", ascending=False)
            .reset_index(drop=True)
        )
        if not site_df.empty:
            analytics["top_site_by_kgco2e"] = site_df.iloc[0].to_dict()
            analytics["top_sites_by_kgco2e"] = site_df.head(5).to_dict(orient="records")

    if "scope" in report_df.columns:
        scope_df = (
            report_df
            .groupby("scope", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
            .sum()
            .sort_values("emissions_kgCO2e", ascending=False)
            .reset_index(drop=True)
        )
        if not scope_df.empty:
            analytics["top_scope_by_kgco2e"] = scope_df.iloc[0].to_dict()

    return analytics


def build_plain_language_takeaways(report_df, batch_result, summary_scope, analytics):
    takeaways = []

    total_t = round(float(batch_result.get("total_tCO2e", 0.0)), 2)
    total_kg = round(float(batch_result.get("total_kgCO2e", 0.0)), 2)
    data_quality = batch_result.get("data_quality", {})
    confidence = batch_result.get("confidence", {})

    if total_kg > 0:
        takeaways.append(
            f"Total reported emissions are {total_t} tCO2e ({total_kg} kgCO2e) across the processed dataset."
        )

    if not summary_scope.empty:
        top_scope = summary_scope.iloc[0]
        takeaways.append(
            f"{top_scope['scope']} is the largest contributor, representing {round(float(top_scope['percent_of_total']), 2)}% of total emissions."
        )

    top_category = analytics.get("top_category_by_kgco2e")
    if top_category:
        takeaways.append(
            f"The highest-emitting category is {top_category.get('category')}, contributing {round(float(top_category.get('emissions_kgCO2e', 0.0)), 2)} kgCO2e."
        )

    top_site = analytics.get("top_site_by_kgco2e")
    if top_site:
        takeaways.append(
            f"The highest-emitting site is {top_site.get('site_name')}, contributing {round(float(top_site.get('emissions_kgCO2e', 0.0)), 2)} kgCO2e."
        )

    coverage = data_quality.get("coverage_percent", 0.0)
    if coverage > 0:
        takeaways.append(
            f"Data processing coverage is {coverage}%, which indicates how much of the uploaded dataset was successfully included in the calculation."
        )

    if confidence:
        takeaways.append(
            f"The current confidence score is {confidence.get('score', 0)}/100, rated {confidence.get('label', 'Low')}."
        )

    return takeaways[:6]


def build_actionable_insights(report_df, batch_result, summary_scope, analytics):
    insights = []
    data_quality = batch_result.get("data_quality", {})
    confidence = batch_result.get("confidence", {})
    unmapped_categories = batch_result.get("unmapped_categories", [])

    if not summary_scope.empty:
        top_scope = summary_scope.iloc[0]
        insights.append({
            "title": "Largest emission driver",
            "body": (
                f"{top_scope['scope']} is currently the biggest contributor at "
                f"{round(float(top_scope['emissions_kgCO2e']), 2)} kgCO2e "
                f"({round(float(top_scope['percent_of_total']), 2)}% of total emissions). "
                f"This should be the first focus area for reduction planning."
            )
        })

    top_category = analytics.get("top_category_by_kgco2e")
    if top_category:
        insights.append({
            "title": "Priority reduction opportunity",
            "body": (
                f"The category with the greatest impact is {top_category.get('category')}. "
                f"Targeting this area is likely to create the largest near-term reduction effect."
            )
        })

    top_site = analytics.get("top_site_by_kgco2e")
    if top_site:
        insights.append({
            "title": "Operational hotspot",
            "body": (
                f"{top_site.get('site_name')} appears to be the highest-emitting site. "
                f"A site-specific review could help identify concentrated savings opportunities."
            )
        })

    if data_quality.get("coverage_percent", 0.0) < 90 and data_quality.get("total_rows", 0) > 0:
        insights.append({
            "title": "Data completeness risk",
            "body": (
                f"Coverage is currently {data_quality.get('coverage_percent', 0.0)}%. "
                f"This suggests some records were excluded, so totals may be incomplete until input issues are corrected."
            )
        })

    if data_quality.get("estimated_rows", 0) > 0:
        insights.append({
            "title": "Estimated calculations present",
            "body": (
                f"{data_quality.get('estimated_rows', 0)} row(s) rely on inferred mapping or non-exact treatment. "
                f"These should be reviewed before external reporting or audit use."
            )
        })

    if data_quality.get("starter_factor_rows", 0) > 0:
        insights.append({
            "title": "Placeholder factor library in use",
            "body": (
                f"{data_quality.get('starter_factor_rows', 0)} row(s) were calculated using starter placeholder factors "
                f"for business travel or waste. Replacing these with your official DEFRA source table will improve trust and defensibility."
            )
        })

    if unmapped_categories:
        top_unmapped = unmapped_categories[0]
        insights.append({
            "title": "Category mapping improvement needed",
            "body": (
                f"Some categories remain unmapped. For example, '{top_unmapped.get('category')}' "
                f"appears {top_unmapped.get('count', 0)} time(s). Expanding the alias mapping will improve coverage and confidence."
            )
        })

    if confidence and confidence.get("score", 0) < 60:
        insights.append({
            "title": "Low-confidence result",
            "body": (
                f"The current confidence score is {confidence.get('score', 0)}/100. "
                f"Input cleanup and replacement of placeholder factors should be prioritised before relying on this result for external communication."
            )
        })

    return insights[:8]


def build_methodology_summary(batch_result):
    data_quality = batch_result.get("data_quality", {})
    starter_rows = int(data_quality.get("starter_factor_rows", 0) or 0)
    estimated_rows = int(data_quality.get("estimated_rows", 0) or 0)

    summary = {
        "framework": "GHG Protocol aligned reporting structure using uploaded activity data and emissions factors.",
        "factor_basis": "Fuel, electricity, and water calculations use known factor tables in this engine. Business travel and waste currently rely on starter placeholder tables until replaced by the official DEFRA 2025 source tables.",
        "calculation_logic": "Each valid input row is mapped to a canonical emissions category, matched to an emissions factor, multiplied by the activity quantity, and aggregated into total CO2e outputs.",
        "assumptions": [],
        "interpretation_notes": []
    }

    if estimated_rows > 0:
        summary["assumptions"].append(
            "Some rows were estimated based on inferred category mapping or non-exact treatment."
        )

    if starter_rows > 0:
        summary["assumptions"].append(
            "Some Scope 3 rows used starter placeholder factor tables and should be replaced with the official DEFRA factors before final reporting."
        )

    summary["interpretation_notes"].append(
        "Results should be read alongside the data quality and confidence sections, especially when rows were excluded or estimated."
    )
    summary["interpretation_notes"].append(
        "Unmapped categories and factor-matching failures should be corrected in the source data for improved completeness."
    )

    return summary


# =========================
# RESPONSE BUILDERS
# =========================
def build_final_response(batch_result):
    report_df = batch_result["report"]

    summary_scope_category = summarize_report_by_scope_and_category(report_df)
    summary_scope = summarize_report_by_scope(report_df)
    factor_transparency = build_factor_transparency(report_df)
    analytics = build_analytics(report_df)

    error_groups = group_errors(batch_result["errors"])
    error_summary = build_error_summary(batch_result["errors"])
    errors_preview = build_errors_preview(batch_result["errors"], max_items=10)

    top_categories = summarize_top_categories(report_df, top_n=5)
    top_sites = summarize_top_sites(report_df, top_n=5)
    factor_quality_summary = summarize_factor_quality(report_df)
    methodology_summary = build_methodology_summary(batch_result)
    takeaways = build_plain_language_takeaways(report_df, batch_result, summary_scope, analytics)
    actionable_insights = build_actionable_insights(report_df, batch_result, summary_scope, analytics)

    return {
        "line_items": report_df.to_dict(orient="records"),
        "summary_by_scope_category": summary_scope_category.to_dict(orient="records"),
        "summary_by_scope": summary_scope.to_dict(orient="records"),
        "totals": {
            "total_kgCO2e": batch_result["total_kgCO2e"],
            "total_tCO2e": batch_result["total_tCO2e"]
        },
        "analytics": analytics,
        "top_categories_by_kgco2e": top_categories,
        "top_sites_by_kgco2e": top_sites,
        "plain_language_takeaways": takeaways,
        "actionable_insights": actionable_insights,
        "confidence_score": batch_result.get("confidence", {}),
        "data_quality": batch_result.get("data_quality", {}),
        "errors": batch_result["errors"],
        "errors_preview": errors_preview,
        "error_summary": error_summary,
        "issue_groups": error_groups,
        "unmapped_categories": batch_result.get("unmapped_categories", []),
        "factor_transparency": factor_transparency,
        "factor_quality_summary": factor_quality_summary,
        "methodology_summary": methodology_summary
    }


def make_json_safe(obj):
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    return obj


def run_emissions_engine(request_json):
    activities = request_json.get("activities", [])
    batch_result = calculate_emissions_batch_safe(activities)
    final_response = build_final_response(batch_result)
    return make_json_safe(final_response)

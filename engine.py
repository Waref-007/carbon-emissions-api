import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
import re
from enum import Enum

class DEFRAYear(Enum):
    """Supported DEFRA factor years."""
    DEFRA_2023 = 2023
    DEFRA_2024 = 2024
    DEFRA_2025 = 2025

# =========================
# FACTOR TABLES
# =========================

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

# DEFRA 2025 VEHICLE FACTORS (Scope 3)
df_vehicle_emissions = pd.DataFrame([
    {"Activity": "Business travel", "Vehicle Type": "Petrol car", "Unit": "vehicle_km", "kg CO2e": 0.192, "Scope": "Scope 3", "DEFRA Source": "DEFRA 2025"},
    {"Activity": "Business travel", "Vehicle Type": "Diesel car", "Unit": "vehicle_km", "kg CO2e": 0.158, "Scope": "Scope 3", "DEFRA Source": "DEFRA 2025"},
    {"Activity": "Business travel", "Vehicle Type": "Electric car (BEV)", "Unit": "vehicle_km", "kg CO2e": 0.076, "Scope": "Scope 3", "DEFRA Source": "DEFRA 2025"},
    {"Activity": "Business travel", "Vehicle Type": "Hybrid (HEV/PHEV)", "Unit": "vehicle_km", "kg CO2e": 0.112, "Scope": "Scope 3", "DEFRA Source": "DEFRA 2025"},
    {"Activity": "Business travel", "Vehicle Type": "HGV (44 tonnes)", "Unit": "vehicle_km", "kg CO2e": 0.975, "Scope": "Scope 3", "DEFRA Source": "DEFRA 2025"},
    {"Activity": "Business travel", "Vehicle Type": "Light commercial vehicle", "Unit": "vehicle_km", "kg CO2e": 0.238, "Scope": "Scope 3", "DEFRA Source": "DEFRA 2025"},
    {"Activity": "Business travel", "Vehicle Type": "Bus", "Unit": "passenger_km", "kg CO2e": 0.089, "Scope": "Scope 3", "DEFRA Source": "DEFRA 2025"},
    {"Activity": "Business travel", "Vehicle Type": "Flight (short-haul)", "Unit": "passenger_km", "kg CO2e": 0.255, "Scope": "Scope 3", "DEFRA Source": "DEFRA 2025"},
    {"Activity": "Business travel", "Vehicle Type": "Flight (medium-haul)", "Unit": "passenger_km", "kg CO2e": 0.195, "Scope": "Scope 3", "DEFRA Source": "DEFRA 2025"},
    {"Activity": "Business travel", "Vehicle Type": "Flight (long-haul)", "Unit": "passenger_km", "kg CO2e": 0.156, "Scope": "Scope 3", "DEFRA Source": "DEFRA 2025"},
    {"Activity": "Business travel", "Vehicle Type": "Rail", "Unit": "passenger_km", "kg CO2e": 0.035, "Scope": "Scope 3", "DEFRA Source": "DEFRA 2025"},
    {"Activity": "Business travel", "Vehicle Type": "Hotel stay", "Unit": "room_nights", "kg CO2e": 10.40, "Scope": "Scope 3", "DEFRA Source": "DEFRA 2025"},
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
# COLUMN MAPPING & AUTO-DETECTION
# =========================
COLUMN_ALIASES = {
    # Energy column variations
    "energy": "amount",
    "consumption": "amount",
    "usage": "amount",
    "kwh": "amount",
    "electricity": "amount",
    "power": "amount",
    "kwh consumed": "amount",
    "total energy": "amount",
    "monthly consumption": "amount",
    "yearly consumption": "amount",
    "annual consumption": "amount",
    
    # Unit column variations
    "unit of measurement": "unit",
    "measurement unit": "unit",
    "uom": "unit",
    
    # Category column variations
    "type": "category",
    "activity type": "category",
    "emission source": "category",
    "scope": "category",
    
    # Date/Period
    "month": "reporting_period",
    "period": "reporting_period",
    "date": "reporting_period",
    "reporting date": "reporting_period",
    "year": "reporting_period",
    "reporting month": "reporting_period",
}

FUEL_ALIASES = {
    "natural gas": "Natural gas",
    "gas": "Natural gas",
    "ng": "Natural gas",
}

UNIT_ALIASES = {
    "kwh (gross cv)": "kWh (Gross CV)",
    "gross cv": "kWh (Gross CV)",
    "gross": "kWh (Gross CV)",
    "kwh gross": "kWh (Gross CV)",
    "kwh ": "kWh (Gross CV)",
    " kwh ": "kWh (Gross CV)",
    "kwh gross cv": "kWh (Gross CV)",
    "kwh": "kWh",
    "kwhh": "kWh",
    "kw h": "kWh",

    "kwh (net cv)": "kWh (Net CV)",
    "net cv": "kWh (Net CV)",
    "net": "kWh (Net CV)",

    "litre": "litres",
    "litres": "litres",
    "l": "litres",

    "cubic metre": "cubic metres",
    "cubic metres": "cubic metres",
    "m3": "cubic metres",
    "m^3": "cubic metres",

    "tonne": "tonnes",
    "tonnes": "tonnes",
    "t": "tonnes",

    "room night": "room_nights",
    "room nights": "room_nights",
    "night": "room_nights",
    "nights": "room_nights",

    "gbp": "gbp",
    "£": "gbp",
    "spend": "gbp",
    "currency": "gbp",

    "km": "vehicle_km",
    "vehicle km": "vehicle_km",
    "vehicle_km": "vehicle_km",
    "miles": "vehicle_km",

    "passenger km": "passenger_km",
    "passenger_km": "passenger_km",
    "pkm": "passenger_km",

    "kg": "kg",
}

CATEGORY_ALIASES = {
    "fuel": "fuel",
    "gas": "fuel",
    "natural gas": "fuel",

    "electricity": "electricity",
    "power": "electricity",
    "electric": "electricity",
    "elec": "electricity",

    "water": "water",
    "water supply": "water",
    "h2o": "water",

    "travel": "business travel",
    "business travel": "business travel",
    "hotel": "business travel",
    "accommodation": "business travel",
    "flight": "business travel",
    "air travel": "business travel",
    "rail": "business travel",
    "train": "business travel",
    "taxi": "business travel",
    "car travel": "business travel",
    "vehicle": "business travel",
    "transport": "business travel",

    "waste": "waste",
    "general waste": "waste",
    "landfill": "waste",
    "recycling": "waste",
    "incineration": "waste",
}

VEHICLE_TYPE_ALIASES = {
    "petrol": "Petrol car",
    "petrol car": "Petrol car",
    "gasoline": "Petrol car",
    "diesel": "Diesel car",
    "diesel car": "Diesel car",
    "electric": "Electric car (BEV)",
    "ev": "Electric car (BEV)",
    "bev": "Electric car (BEV)",
    "vw id3": "Electric car (BEV)",
    "vw id.3": "Electric car (BEV)",
    "tesla": "Electric car (BEV)",
    "hybrid": "Hybrid (HEV/PHEV)",
    "phev": "Hybrid (HEV/PHEV)",
    "hev": "Hybrid (HEV/PHEV)",
    "plug-in hybrid": "Hybrid (HEV/PHEV)",
    "bmw 330e": "Hybrid (HEV/PHEV)",
    "hgv": "HGV (44 tonnes)",
    "44 tonne": "HGV (44 tonnes)",
    "44-ton": "HGV (44 tonnes)",
    "44 ton": "HGV (44 tonnes)",
    "mercedes actros": "HGV (44 tonnes)",
    "truck": "HGV (44 tonnes)",
    "lorry": "HGV (44 tonnes)",
    "van": "Light commercial vehicle",
    "lcv": "Light commercial vehicle",
    "light commercial": "Light commercial vehicle",
    "bus": "Bus",
    "flight": "Flight (medium-haul)",
    "air": "Flight (medium-haul)",
    "plane": "Flight (medium-haul)",
    "short-haul flight": "Flight (short-haul)",
    "long-haul flight": "Flight (long-haul)",
    "rail": "Rail",
    "train": "Rail",
    "hotel": "Hotel stay",
    "accommodation": "Hotel stay",
}

WASTE_TYPE_ALIASES = {
    "waste": "General waste",
    "general waste": "General waste",
    "landfill": "Landfill waste",
    "landfill waste": "Landfill waste",
    "recycling": "Recycling",
    "recycled": "Recycling",
    "incineration": "Incineration",
    "incinerated": "Incineration",
}


# =========================
# CONSTANTS
# =========================
DEFAULT_FACTOR_YEAR = 2025
SUPPORTED_CATEGORY_HINT = "Supported categories: fuel, electricity, water, business travel, waste."
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
# AUTO-DETECTION & COLUMN MAPPING
# =========================
def detect_and_remap_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Auto-detect and remap columns from flexible input formats.
    
    Returns:
        - Cleaned dataframe with standardized columns
        - Mapping dict showing what was remapped
    """
    df_remapped = df.copy()
    mapping_applied = {}
    
    original_cols = set(df_remapped.columns)
    lower_cols = {col.lower(): col for col in df_remapped.columns}
    
    for alias, standard_col in COLUMN_ALIASES.items():
        if standard_col not in df_remapped.columns:
            if alias in lower_cols:
                original_col = lower_cols[alias]
                df_remapped = df_remapped.rename(columns={original_col: standard_col})
                mapping_applied[original_col] = standard_col
    
    # If still no "amount" column, try to auto-detect numeric columns
    if "amount" not in df_remapped.columns:
        numeric_cols = df_remapped.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            first_numeric = numeric_cols[0]
            df_remapped = df_remapped.rename(columns={first_numeric: "amount"})
            mapping_applied[first_numeric] = "amount"
    
    return df_remapped, mapping_applied


def auto_detect_category_from_data(row: Dict) -> Optional[str]:
    """
    Attempt to infer category from any available fields if explicit category is missing.
    """
    candidates = [
        row.get("category"),
        row.get("item_name"),
        row.get("sub_category"),
        row.get("notes"),
        row.get("vehicle_type"),
    ]
    
    for val in candidates:
        if val:
            normalized = normalize_category(val)
            if normalized:
                return normalized
    
    return None


# =========================
# NORMALIZATION HELPERS
# =========================
def normalize_fuel(fuel):
    key = str(fuel).strip().lower()
    return FUEL_ALIASES.get(key, fuel)


def normalize_unit(unit):
    if not unit:
        return unit
    key = str(unit).strip().lower()
    return UNIT_ALIASES.get(key, unit)


def normalize_category(category):
    if not category:
        return None
    key = str(category).strip().lower()
    return CATEGORY_ALIASES.get(key, None)


def normalize_vehicle_type(value):
    if value is None:
        return None
    key = str(value).strip().lower()
    return VEHICLE_TYPE_ALIASES.get(key, None)


def normalize_waste_type(value):
    if value is None:
        return None
    key = str(value).strip().lower()
    return WASTE_TYPE_ALIASES.get(key, None)


# =========================
# INFERENCE HELPERS
# =========================
def infer_canonical_category(activity):
    """Improved inference that checks multiple sources."""
    raw_category = safe_text(activity.get("category"))
    inferred = auto_detect_category_from_data(activity)
    
    if inferred:
        return {
            "raw_category": raw_category,
            "canonical_category": inferred,
            "mapping_source": "category" if raw_category else "inferred",
            "mapping_confidence": "high" if raw_category else "medium"
        }
    
    return {
        "raw_category": raw_category,
        "canonical_category": None,
        "mapping_source": "unmapped",
        "mapping_confidence": "low"
    }


def classify_calculation_basis(mapping_source, factor_quality):
    if factor_quality == "official" and mapping_source == "category":
        return "exact"
    if factor_quality == "official" and mapping_source == "inferred":
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
    mapping_confidence=None,
    defra_reference=None,
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
        "defra_reference": defra_reference,
        "factor_name": factor_name or item_name,
        "factor_description": factor_description or "",
        "factor_quality": factor_quality,
        "original_category": original_category,
        "mapping_source": mapping_source,
        "mapping_confidence": mapping_confidence,
        "calculation_basis": calculation_basis
    }])


# =========================
# CALCULATORS (IMPROVED)
# =========================
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
    """Improved to handle flexible unit inputs."""
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
        mapping_confidence=mapping_confidence,
        defra_reference="DEFRA 2025 Electricity Grid Factor"
    )


def calculate_vehicle_emissions_unified(
    df,
    vehicle_type,
    unit,
    amount,
    factor_year=DEFAULT_FACTOR_YEAR,
    original_category=None,
    mapping_source=None,
    mapping_confidence=None
):
    """NEW: Specific vehicle-class calculations with DEFRA factors."""
    vehicle_type = validate_text_input(vehicle_type, "vehicle_type")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)

    vehicle_normalized = normalize_vehicle_type(vehicle_type)
    unit_normalized = normalize_unit(unit)

    if not vehicle_normalized:
        raise ValueError(
            f"Unknown vehicle type: '{vehicle_type}'. "
            f"Supported: Petrol car, Diesel car, Electric car (BEV), Hybrid (HEV/PHEV), "
            f"HGV (44 tonnes), Light commercial vehicle, Bus, Flight (short/medium/long-haul), Rail, Hotel stay."
        )

    match = df[
        (df["Vehicle Type"].str.strip().str.lower() == vehicle_normalized.strip().lower()) &
        (df["Unit"].str.strip().str.lower() == unit_normalized.strip().lower())
    ].copy()

    if match.empty:
        raise ValueError(
            f"No matching vehicle factor found for '{vehicle_normalized}' with unit '{unit_normalized}'. "
            f"Please check vehicle type and unit combination."
        )

    row = match.iloc[0]
    factor = float(row["kg CO2e"])
    emissions_kg = amount * factor

    return build_result_row(
        category="Business Travel",
        sub_category=row["Activity"],
        item_name=row["Vehicle Type"],
        scope=row["Scope"],
        input_amount=amount,
        input_unit=unit,
        normalized_unit=row["Unit"],
        emission_factor=factor,
        factor_year=factor_year,
        emissions_kg=emissions_kg,
        data_source="DEFRA 2025",
        factor_name=f"{row['Vehicle Type']} / {row['Unit']}",
        factor_description=f"Vehicle-specific factor: {row['Vehicle Type']}",
        factor_quality="official",
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence,
        defra_reference=row.get("DEFRA Source", "DEFRA 2025")
    )


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
        mapping_confidence=mapping_confidence,
        defra_reference="DEFRA 2025 Fuel Factors"
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

    unit_normalized = normalize_unit(unit)

    match = df[
        (df["Type"].str.strip().str.lower() == water_type.strip().lower()) &
        (df["Unit"].str.strip().str.lower() == unit_normalized.strip().lower())
    ].copy()

    if match.empty:
        raise ValueError(
            f"No matching water factor found for Type='{water_type}' and Unit='{unit_normalized}'"
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
        mapping_confidence=mapping_confidence,
        defra_reference="DEFRA 2025 Water Factors"
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
        data_source="DEFRA 2025",
        factor_name=f"{row['Waste Type']} / {row['Unit']}",
        factor_description="Waste disposal factor",
        factor_quality="official",
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence,
        defra_reference="DEFRA 2025 Waste Factors"
    )


# =========================
# PREPROCESSING
# =========================
def preprocess_uploaded_dataframe(df):
    """Enhanced preprocessing with column auto-detection."""
    df_clean, col_mapping = detect_and_remap_columns(df)
    
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

    df_clean = df_clean.where(pd.notnull(df_clean), None)
    
    return df_clean, col_mapping


# =========================
# DISPATCHER (IMPROVED)
# =========================
def calculate_emissions(activity):
    """Improved dispatcher with vehicle-specific routing."""
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
        # NEW: Check if vehicle_type is specified for vehicle-specific calculation
        vehicle_type = activity.get("vehicle_type") or activity.get("item_name")
        
        if vehicle_type:
            normalized_vehicle = normalize_vehicle_type(vehicle_type)
            if normalized_vehicle:
                return calculate_vehicle_emissions_unified(
                    df=df_vehicle_emissions,
                    vehicle_type=vehicle_type,
                    unit=unit,
                    amount=amount,
                    factor_year=activity.get("factor_year", DEFAULT_FACTOR_YEAR),
                    original_category=original_category,
                    mapping_source=mapping_source,
                    mapping_confidence=mapping_confidence
                )
        
        # Fallback to generic if no vehicle type
        raise ValueError(
            f"Business travel requires vehicle_type. "
            f"Supported: Petrol car, Diesel car, Electric car (BEV), Hybrid (HEV/PHEV), "
            f"HGV (44 tonnes), Light commercial vehicle, Bus, Flight, Rail, Hotel stay."
        )

    if canonical_category == "waste":
        waste_type = activity.get("waste_type") or activity.get("item_name") or "General waste"
        normalized_waste = normalize_waste_type(waste_type)
        if not normalized_waste:
            normalized_waste = waste_type
        
        return calculate_waste_emissions_unified(
            df=df_waste_clean,
            waste_type=normalized_waste,
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
    if "no matching" in msg or "factor" in msg:
        return "Factor matching failure"
    if "year must be" in msg or "year is out of valid range" in msg:
        return "Invalid year"
    if "required" in msg:
        return "Missing required field"
    if "unknown vehicle" in msg or "vehicle type" in msg:
        return "Invalid vehicle type"
    return "Other validation issue"


def infer_severity(issue_type, count=1):
    high_types = {
        "Unmapped category",
        "Factor matching failure",
        "Missing required field",
        "Missing unit",
        "Missing amount",
        "Non-numeric amount",
        "Invalid vehicle type",
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
        "Unmapped category": "Standardise the category labels in the source data so they match the calculator taxonomy.",
        "Missing amount": "Add a valid numeric amount for the affected rows before re-uploading.",
        "Non-numeric amount": "Replace text or malformed values in the amount field with clean numeric values.",
        "Negative amount": "Check whether the input quantity should be positive.",
        "Missing unit": "Populate the missing unit. Supported: kWh, vehicle_km, passenger_km, tonnes, room_nights, gbp, etc.",
        "Factor matching failure": "Review category, vehicle type, and unit combination for correctness.",
        "Invalid year": "Use a valid integer year within the supported range (1900-2100).",
        "Missing required field": "Complete the missing required columns before submitting.",
        "Invalid vehicle type": "Use a supported vehicle type: Petrol car, Diesel car, Electric car (BEV), Hybrid, HGV, Bus, Flight, Rail, Hotel stay.",
        "Other validation issue": "Review the affected rows and correct the source data before re-uploading."
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

    if total_rows <= 0:
        return {
            "score": 0,
            "label": "Low",
            "coverage_percent": 0.0,
            "notes": ["No valid rows were available for calculation."]
        }

    score = 100.0
    score -= max(0.0, 100.0 - coverage_percent) * 0.50
    score -= (inferred_rows / total_rows) * 10.0
    score -= (estimated_rows / total_rows) * 15.0

    score = round(max(0.0, min(100.0, score)), 0)

    if score >= 85:
        label = "High"
    elif score >= 65:
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

    if not notes:
        notes.append("All rows processed with official DEFRA factors.")

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
    official_factor_rows = 0

    if not final_report.empty:
        exact_factor_rows = int((final_report["calculation_basis"] == "exact").sum()) if "calculation_basis" in final_report.columns else 0
        estimated_rows = int((final_report["calculation_basis"] == "estimated").sum()) if "calculation_basis" in final_report.columns else 0
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


def build_factor_transparency(report_df):
    """Enhanced factor transparency with DEFRA references."""
    if report_df.empty:
        return []

    cols = [
        "category",
        "scope",
        "item_name",
        "factor_name",
        "emission_factor_kgCO2e_per_unit",
        "normalized_unit",
        "factor_year",
        "data_source",
        "defra_reference",
        "factor_quality",
        "calculation_basis"
    ]

    available_cols = [c for c in cols if c in report_df.columns]
    factor_df = report_df[available_cols].drop_duplicates().reset_index(drop=True)

    factor_df = factor_df.sort_values(
        by=[c for c in ["category", "item_name"] if c in factor_df.columns]
    ).reset_index(drop=True)

    return factor_df.to_dict(orient="records")


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


def build_factor_log(report_df):
    """NEW: Detailed log showing exactly which DEFRA factors were applied."""
    if report_df.empty:
        return []
    
    cols_to_show = [
        "row_index",
        "item_name",
        "input_amount",
        "input_unit",
        "normalized_unit",
        "emission_factor_kgCO2e_per_unit",
        "emissions_kgCO2e",
        "defra_reference",
        "factor_quality",
        "category"
    ]
    
    available = [c for c in cols_to_show if c in report_df.columns]
    factor_log = report_df[available].copy()
    
    return factor_log.to_dict(orient="records")


# =========================
# RESPONSE BUILDERS
# =========================
def build_final_response(batch_result):
    report_df = batch_result["report"]

    summary_scope_category = summarize_report_by_scope_and_category(report_df)
    summary_scope = summarize_report_by_scope(report_df)
    factor_transparency = build_factor_transparency(report_df)
    factor_log = build_factor_log(report_df)
    analytics = build_analytics(report_df)

    error_groups = group_errors(batch_result["errors"])
    error_summary = build_error_summary(batch_result["errors"])
    errors_preview = build_errors_preview(batch_result["errors"], max_items=10)

    top_categories = summarize_top_categories(report_df, top_n=5)

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
        "confidence_score": batch_result.get("confidence", {}),
        "data_quality": batch_result.get("data_quality", {}),
        "errors": batch_result["errors"],
        "errors_preview": errors_preview,
        "error_summary": error_summary,
        "issue_groups": error_groups,
        "unmapped_categories": batch_result.get("unmapped_categories", []),
        "factor_transparency": factor_transparency,
        "factor_log": factor_log,
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
    """Main entry point."""
    activities = request_json.get("activities", [])
    batch_result = calculate_emissions_batch_safe(activities)
    final_response = build_final_response(batch_result)
    return make_json_safe(final_response)


# =========================
# CSV TEMPLATE GENERATOR
# =========================
def generate_upload_template() -> str:
    """Generate a branded CSV template for clients."""
    template = """category,item_name,amount,unit,vehicle_type,notes,reporting_period,site_name
Electricity,UK Grid Supply,1200,kWh,,January 2024 consumption,2024-01-31,London HQ
Business Travel,Fleet Vehicle,150,vehicle_km,Electric car (BEV),VW ID3,2024-01-31,London HQ
Business Travel,HGV Transport,500,vehicle_km,HGV (44 tonnes),Mercedes Actros,2024-01-31,London HQ
Business Travel,Hybrid Fleet,200,vehicle_km,Hybrid (HEV/PHEV),BMW 330e,2024-01-31,London HQ
Waste,General,2.5,tonnes,,Monthly disposal,2024-01-31,London HQ
Water,UK Supply,45,cubic metres,,Monthly usage,2024-01-31,London HQ
"""
    return template


def validate_upload_compatibility(df: pd.DataFrame) -> Dict[str, Any]:
    """NEW: Validation report for uploaded data BEFORE processing."""
    df_test, col_mapping = detect_and_remap_columns(df)
    
    validation_report = {
        "columns_detected": list(df.columns),
        "columns_remapped": col_mapping,
        "required_columns_present": all(col in df_test.columns for col in ["amount", "unit", "category"]),
        "total_rows": len(df),
        "sample_preview": df_test.head(3).to_dict(orient="records"),
        "potential_issues": []
    }
    
    if "amount" not in df_test.columns:
        validation_report["potential_issues"].append("No 'amount' column detected. Please provide a numeric energy/quantity column.")
    
    if "unit" not in df_test.columns:
        validation_report["potential_issues"].append("No 'unit' column detected. Please specify units (kWh, vehicle_km, tonnes, etc.).")
    
    if "category" not in df_test.columns:
        validation_report["potential_issues"].append("No 'category' column detected. Please specify activity type (Electricity, Business Travel, etc.).")
    
    return validation_report

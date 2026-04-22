
import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# =========================
# FACTOR TABLES
# =========================
# These defaults preserve your existing known factors and add a small,
# explicit library for direct liquid-fuel combustion so monthly fleet-fuel
# uploads can be calculated instead of silently failing.
#
# IMPORTANT:
# - Fuel, electricity, water and direct liquid-fuel combustion are the
#   highest-confidence paths in this file.
# - Business-travel and waste tables below remain starter tables unless you
#   replace them with your official DEFRA dataset.
# - Vehicle-specific mileage factors can be injected at runtime using
#   request_json["vehicle_factor_overrides"].

df_fuels_clean = pd.DataFrame([
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "kWh (Gross CV)", "kg CO2e": 0.18296},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "kWh (Net CV)", "kg CO2e": 0.20270},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "cubic metres", "kg CO2e": 2.06672},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "tonnes", "kg CO2e": 2575.46441},
])

df_liquid_fuels_clean = pd.DataFrame([
    {"Activity": "Liquid fuels", "Fuel": "Diesel", "Unit": "litres", "kg CO2e": 2.687, "Scope": "Scope 1"},
    {"Activity": "Liquid fuels", "Fuel": "Petrol", "Unit": "litres", "kg CO2e": 2.193, "Scope": "Scope 1"},
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
    "diesel": "Diesel",
    "gas oil": "Diesel",
    "road diesel": "Diesel",
    "derv": "Diesel",
    "petrol": "Petrol",
    "gasoline": "Petrol",
}

UNIT_ALIASES = {
    "kwh (gross cv)": "kWh (Gross CV)",
    "gross cv": "kWh (Gross CV)",
    "gross": "kWh (Gross CV)",
    "kwh gross": "kWh (Gross CV)",
    "kwh gross cv": "kWh (Gross CV)",

    "kwh (net cv)": "kWh (Net CV)",
    "net cv": "kWh (Net CV)",
    "net": "kWh (Net CV)",

    "kwh": "kWh",

    "litre": "litres",
    "litres": "litres",
    "liter": "litres",
    "liters": "litres",
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

    "mile": "miles",
    "miles": "miles",
    "mi": "miles",

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
    "gaseous fuel": "fuel",

    "liquid fuel": "liquid fuel",
    "diesel": "liquid fuel",
    "petrol": "liquid fuel",
    "fleet fuel": "liquid fuel",

    "electricity": "electricity",
    "power": "electricity",
    "ev charging electricity": "electricity",

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
    "business miles": "business travel",

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

VEHICLE_CLASS_RULES = [
    ("car_ev", [r"\belectric\b", r"\bid3\b", r"\beqa\b", r"\bev\b"]),
    ("car_plugin_hybrid", [r"plug[\s\-]?in hybrid", r"\b330e\b", r"\bgte\b"]),
    ("hgv_44t", [r"\b44\s*ton\b", r"\bactros\b", r"articulated unit"]),
    ("hgv_rigid_26t", [r"\b26\s*ton\b", r"\brigid\b"]),
    ("pickup_4x4", [r"ranger", r"wildtrak", r"pickup"]),
]


# =========================
# CONSTANTS
# =========================
DEFAULT_FACTOR_YEAR = 2025
DEFAULT_COUNTRY = "Electricity: UK"
SUPPORTED_CATEGORY_HINT = "Supported / mappable categories include: fuel, liquid fuel, electricity, water, travel, hotel, waste."

METADATA_FIELDS = [
    "record_id",
    "client_name",
    "site_name",
    "reporting_period",
    "notes",
    "source_file",
    "row_index_original",
    "source_section",
    "source_sheet",
    "source_workbook",
    "period_start",
    "period_end",
    "invoice_reference",
    "raw_vehicle",
    "vehicle_class",
    "vehicle_registration",
    "driver_name",
]


# =========================
# VALIDATION HELPERS
# =========================
def validate_text_input(value: Any, field_name: str) -> str:
    if value is None:
        raise ValueError(f"{field_name} is required.")
    value = str(value).strip()
    if value == "" or value.lower() == "nan":
        raise ValueError(f"{field_name} is required.")
    return value


def validate_amount(amount: Any) -> float:
    if amount is None:
        raise ValueError("amount is required.")
    try:
        if isinstance(amount, str):
            amount = amount.replace(",", "").replace("£", "").strip()
        amount = float(amount)
    except Exception:
        raise ValueError("amount must be numeric.")
    if amount < 0:
        raise ValueError("amount cannot be negative.")
    return amount


def validate_year(year: Any) -> int:
    if year is None:
        return DEFAULT_FACTOR_YEAR
    try:
        year = int(float(year))
    except Exception:
        raise ValueError("year must be an integer.")
    if year < 1900 or year > 2100:
        raise ValueError("year is out of valid range.")
    return year


def safe_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    value = str(value).strip()
    if value == "" or value.lower() == "nan":
        return None
    return value


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").replace("£", "").strip()
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return True
    return False


def looks_like_number(value: Any) -> bool:
    if value is None or is_blank(value):
        return False
    try:
        if isinstance(value, str):
            float(value.replace(",", "").replace("£", "").strip())
        else:
            float(value)
        return True
    except Exception:
        return False


def looks_like_date(value: Any) -> bool:
    if is_blank(value):
        return False
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return True
    try:
        parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
        return pd.notna(parsed)
    except Exception:
        return False


def parse_date(value: Any) -> Optional[pd.Timestamp]:
    if is_blank(value):
        return None
    try:
        parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
        if pd.isna(parsed):
            return None
        return parsed
    except Exception:
        return None


def parse_period_bounds(value: Any) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    text = safe_text(value)
    if not text:
        return None, None

    if looks_like_date(text):
        dt = parse_date(text)
        return dt, dt

    parts = re.split(r"\s*-\s*", text)
    if len(parts) == 2:
        start = parse_date(parts[0])
        end = parse_date(parts[1])
        return start, end

    return None, None


def first_non_blank(*values: Any) -> Any:
    for value in values:
        if not is_blank(value):
            return value
    return None


def serialise_timestamp(value: Optional[pd.Timestamp]) -> Optional[str]:
    if value is None:
        return None
    return pd.Timestamp(value).date().isoformat()


# =========================
# NORMALIZATION HELPERS
# =========================
def normalize_fuel(fuel: Any) -> Any:
    key = str(fuel).strip().lower()
    return FUEL_ALIASES.get(key, fuel)


def normalize_unit(unit: Any) -> Any:
    key = str(unit).strip().lower()
    return UNIT_ALIASES.get(key, unit)


def normalize_water_type(water_type: Any) -> Any:
    key = str(water_type).strip().lower()
    return WATER_TYPE_ALIASES.get(key, water_type)


def normalize_water_unit(unit: Any) -> Any:
    key = str(unit).strip().lower()
    return WATER_UNIT_ALIASES.get(key, unit)


def normalize_category(category: Any) -> Optional[str]:
    key = str(category).strip().lower()
    return CATEGORY_ALIASES.get(key, None)


def normalize_travel_type(value: Any) -> Optional[str]:
    if value is None:
        return None
    key = str(value).strip().lower()
    return TRAVEL_TYPE_ALIASES.get(key, None)


def normalize_waste_type(value: Any) -> Optional[str]:
    if value is None:
        return None
    key = str(value).strip().lower()
    return WASTE_TYPE_ALIASES.get(key, None)


def classify_vehicle_class(description: Any) -> Optional[str]:
    text = safe_text(description)
    if not text:
        return None
    text_lower = text.lower()
    for vehicle_class, patterns in VEHICLE_CLASS_RULES:
        for pattern in patterns:
            if re.search(pattern, text_lower):
                return vehicle_class
    return "unknown_vehicle"


def convert_miles_to_km(value: float) -> float:
    return float(value) * 1.609344


# =========================
# INFERENCE HELPERS
# =========================
def infer_canonical_category(activity: Dict[str, Any]) -> Dict[str, Any]:
    raw_category = safe_text(activity.get("category"))
    raw_item_name = safe_text(activity.get("item_name"))
    raw_sub_category = safe_text(activity.get("sub_category"))
    raw_notes = safe_text(activity.get("notes"))
    raw_source_section = safe_text(activity.get("source_section"))

    candidates = [raw_category, raw_item_name, raw_sub_category, raw_notes, raw_source_section]

    for value in candidates:
        if not value:
            continue
        mapped = normalize_category(value)
        if mapped:
            confidence = "high" if value == raw_category else "medium"
            return {
                "raw_category": raw_category or value,
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


def infer_business_travel_type(activity: Dict[str, Any]) -> str:
    raw_category = safe_text(activity.get("category"))
    item_name = safe_text(activity.get("item_name"))
    sub_category = safe_text(activity.get("sub_category"))
    notes = safe_text(activity.get("notes"))
    raw_vehicle = safe_text(activity.get("raw_vehicle"))

    candidates = [raw_category, item_name, sub_category, notes, raw_vehicle]

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


def infer_waste_type(activity: Dict[str, Any]) -> str:
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


def classify_calculation_basis(mapping_source: Optional[str], factor_quality: Optional[str]) -> str:
    factor_quality = (factor_quality or "").lower()
    if factor_quality == "official" and mapping_source == "category":
        return "exact"
    if factor_quality == "official" and mapping_source in {"inferred", "template_parser"}:
        return "estimated"
    if factor_quality in {"runtime_override", "client_override"}:
        return "estimated"
    if factor_quality != "official":
        return "estimated"
    return "estimated"


# =========================
# OUTPUT HELPER
# =========================
def build_result_row(
    category: str,
    sub_category: str,
    item_name: str,
    scope: str,
    input_amount: float,
    input_unit: str,
    normalized_unit: str,
    emission_factor: float,
    factor_year: int,
    emissions_kg: float,
    data_source: str = "DEFRA 2025",
    factor_name: Optional[str] = None,
    factor_description: Optional[str] = None,
    factor_quality: str = "official",
    original_category: Optional[str] = None,
    mapping_source: Optional[str] = None,
    mapping_confidence: Optional[str] = None,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    calculation_basis = classify_calculation_basis(mapping_source, factor_quality)

    row = {
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
        "calculation_basis": calculation_basis,
    }

    if extra_fields:
        row.update(extra_fields)

    return pd.DataFrame([row])


# =========================
# CALCULATORS
# =========================
def calculate_fuel_emissions_unified(
    df: pd.DataFrame,
    fuel: str,
    unit: str,
    amount: float,
    factor_year: int = DEFAULT_FACTOR_YEAR,
    original_category: Optional[str] = None,
    mapping_source: Optional[str] = None,
    mapping_confidence: Optional[str] = None,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    fuel = validate_text_input(fuel, "fuel")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)

    fuel_normalized = normalize_fuel(fuel)
    unit_normalized = normalize_unit(unit)

    match = df[
        (df["Fuel"].str.strip().str.lower() == str(fuel_normalized).strip().lower()) &
        (df["Unit"].str.strip().str.lower() == str(unit_normalized).strip().lower())
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
        extra_fields=extra_fields,
    )


def calculate_liquid_fuel_emissions_unified(
    df: pd.DataFrame,
    fuel: str,
    unit: str,
    amount: float,
    factor_year: int = DEFAULT_FACTOR_YEAR,
    original_category: Optional[str] = None,
    mapping_source: Optional[str] = None,
    mapping_confidence: Optional[str] = None,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    fuel = validate_text_input(fuel, "fuel")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)

    fuel_normalized = normalize_fuel(fuel)
    unit_normalized = normalize_unit(unit)

    match = df[
        (df["Fuel"].str.strip().str.lower() == str(fuel_normalized).strip().lower()) &
        (df["Unit"].str.strip().str.lower() == str(unit_normalized).strip().lower())
    ].copy()

    if match.empty:
        raise ValueError(
            f"No matching liquid-fuel factor found for Fuel='{fuel}' "
            f"(normalized='{fuel_normalized}') and Unit='{unit}' "
            f"(normalized='{unit_normalized}')"
        )

    row = match.iloc[0]
    factor = float(row["kg CO2e"])
    emissions_kg = amount * factor

    return build_result_row(
        category="Liquid Fuel",
        sub_category=row["Activity"],
        item_name=row["Fuel"],
        scope=row.get("Scope", "Scope 1"),
        input_amount=amount,
        input_unit=unit,
        normalized_unit=row["Unit"],
        emission_factor=factor,
        factor_year=factor_year,
        emissions_kg=emissions_kg,
        data_source="Engine default direct fuel factor library",
        factor_name=f"{row['Fuel']} / {row['Unit']}",
        factor_description="Direct liquid-fuel combustion factor",
        factor_quality="official",
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence,
        extra_fields=extra_fields,
    )


def calculate_electricity_emissions_unified(
    df: pd.DataFrame,
    country: str,
    unit: str,
    amount: float,
    year: int = DEFAULT_FACTOR_YEAR,
    original_category: Optional[str] = None,
    mapping_source: Optional[str] = None,
    mapping_confidence: Optional[str] = None,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    country = validate_text_input(country, "country")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)
    year = validate_year(year)

    unit_normalized = normalize_unit(unit)

    match = df[
        (df["Country"].str.strip().str.lower() == country.strip().lower()) &
        (df["Unit"].str.strip().str.lower() == str(unit_normalized).strip().lower()) &
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
        extra_fields=extra_fields,
    )


def calculate_water_emissions_unified(
    df: pd.DataFrame,
    water_type: str,
    unit: str,
    amount: float,
    factor_year: int = DEFAULT_FACTOR_YEAR,
    original_category: Optional[str] = None,
    mapping_source: Optional[str] = None,
    mapping_confidence: Optional[str] = None,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    water_type = validate_text_input(water_type, "water_type")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)

    water_type_normalized = normalize_water_type(water_type)
    unit_normalized = normalize_water_unit(unit)

    match = df[
        (df["Type"].str.strip().str.lower() == str(water_type_normalized).strip().lower()) &
        (df["Unit"].str.strip().str.lower() == str(unit_normalized).strip().lower())
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
        mapping_confidence=mapping_confidence,
        extra_fields=extra_fields,
    )


def calculate_business_travel_emissions_unified(
    df: pd.DataFrame,
    travel_type: str,
    unit: str,
    amount: float,
    factor_year: int = DEFAULT_FACTOR_YEAR,
    original_category: Optional[str] = None,
    mapping_source: Optional[str] = None,
    mapping_confidence: Optional[str] = None,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    travel_type = validate_text_input(travel_type, "travel_type")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)

    unit_normalized = normalize_unit(unit)
    amount_for_factor = amount

    if unit_normalized == "miles":
        unit_normalized = "vehicle_km"
        amount_for_factor = convert_miles_to_km(amount)

    match = df[
        (df["Travel Type"].str.strip().str.lower() == travel_type.strip().lower()) &
        (df["Unit"].str.strip().str.lower() == str(unit_normalized).strip().lower())
    ].copy()

    if match.empty:
        raise ValueError(
            f"No matching business travel factor found for Travel Type='{travel_type}' and Unit='{unit_normalized}'. "
            f"Expected example units include: room_nights, passenger_km, vehicle_km, gbp."
        )

    row = match.iloc[0]
    factor = float(row["kg CO2e"])
    emissions_kg = amount_for_factor * factor

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
        mapping_confidence=mapping_confidence,
        extra_fields=extra_fields,
    )


def calculate_business_travel_with_override(activity: Dict[str, Any], mapping: Dict[str, Any]) -> pd.DataFrame:
    amount = validate_amount(activity.get("amount"))
    unit = validate_text_input(activity.get("unit"), "unit")
    emission_factor = validate_amount(activity.get("emission_factor_override"))
    factor_year = validate_year(activity.get("factor_year_override", activity.get("factor_year", DEFAULT_FACTOR_YEAR)))

    normalized_unit = normalize_unit(unit)
    amount_for_factor = amount
    factor_unit = safe_text(activity.get("factor_unit_override")) or normalized_unit

    if normalized_unit == "miles" and factor_unit == "vehicle_km":
        amount_for_factor = convert_miles_to_km(amount)

    emissions_kg = amount_for_factor * emission_factor

    extra_fields = {
        "vehicle_class": activity.get("vehicle_class"),
        "vehicle_registration": activity.get("vehicle_registration"),
        "driver_name": activity.get("driver_name"),
    }

    return build_result_row(
        category="Business Travel",
        sub_category=safe_text(activity.get("vehicle_class")) or "Vehicle-specific travel",
        item_name=safe_text(activity.get("item_name")) or safe_text(activity.get("raw_vehicle")) or "Business travel",
        scope=safe_text(activity.get("scope_override")) or "Scope 3",
        input_amount=amount,
        input_unit=unit,
        normalized_unit=factor_unit,
        emission_factor=emission_factor,
        factor_year=factor_year,
        emissions_kg=emissions_kg,
        data_source=safe_text(activity.get("data_source_override")) or "Runtime vehicle factor override",
        factor_name=safe_text(activity.get("factor_name_override")) or "Vehicle-specific override",
        factor_description=safe_text(activity.get("factor_description_override")) or "Vehicle-specific mileage factor supplied at runtime",
        factor_quality=safe_text(activity.get("factor_quality_override")) or "runtime_override",
        original_category=mapping.get("raw_category"),
        mapping_source=mapping.get("mapping_source"),
        mapping_confidence=mapping.get("mapping_confidence"),
        extra_fields=extra_fields,
    )


def calculate_waste_emissions_unified(
    df: pd.DataFrame,
    waste_type: str,
    unit: str,
    amount: float,
    factor_year: int = DEFAULT_FACTOR_YEAR,
    original_category: Optional[str] = None,
    mapping_source: Optional[str] = None,
    mapping_confidence: Optional[str] = None,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    waste_type = validate_text_input(waste_type, "waste_type")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)

    unit_normalized = normalize_unit(unit)

    match = df[
        (df["Waste Type"].str.strip().str.lower() == waste_type.strip().lower()) &
        (df["Unit"].str.strip().str.lower() == str(unit_normalized).strip().lower())
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
        mapping_confidence=mapping_confidence,
        extra_fields=extra_fields,
    )


# =========================
# PREPROCESSING
# =========================
def preprocess_uploaded_dataframe(df: pd.DataFrame) -> pd.DataFrame:
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
# WORKBOOK PARSER HELPERS
# =========================
def records_to_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df = df.reset_index(drop=True)
    df.columns = list(range(df.shape[1]))
    return df


def looks_like_canonical_activity_df(df: pd.DataFrame) -> bool:
    canonical_cols = {"category", "amount", "unit"}
    if not canonical_cols.issubset(set(df.columns)):
        return False
    category_non_blank = df["category"].apply(lambda x: not is_blank(x)).sum()
    amount_non_blank = df["amount"].apply(lambda x: not is_blank(x)).sum()
    return bool(category_non_blank > 0 and amount_non_blank > 0)


def cell_text(value: Any) -> str:
    text = safe_text(value)
    return text.lower() if text else ""


def find_matching_cells(df: pd.DataFrame, pattern: str) -> List[Tuple[int, int, str]]:
    matches: List[Tuple[int, int, str]] = []
    regex = re.compile(pattern, flags=re.IGNORECASE)
    for row_idx in range(df.shape[0]):
        for col_idx in range(df.shape[1]):
            value = df.iat[row_idx, col_idx]
            text = safe_text(value)
            if text and regex.search(text):
                matches.append((row_idx, col_idx, text))
    return matches


def detect_workbook_sections(df: pd.DataFrame) -> Dict[str, List[Tuple[int, int, str]]]:
    return {
        "electricity": find_matching_cells(df, r"electricity\s+usage"),
        "gas": find_matching_cells(df, r"gas\s+charges"),
        "fuel_litres": find_matching_cells(df, r"total\s+litres\s+for\s+fuel"),
        "adblue": find_matching_cells(df, r"ad\s*blue"),
        "ev_charging": find_matching_cells(df, r"electric\s+used\s+by\s+ev"),
        "business_miles": find_matching_cells(df, r"business\s+miles"),
        "vehicles": find_matching_cells(df, r"vehicles"),
    }


def build_normalized_activity(
    category: str,
    amount: Any,
    unit: str,
    row_index_original: int,
    source_section: str,
    reporting_period: Optional[str] = None,
    source_file: Optional[str] = None,
    source_sheet: Optional[str] = None,
    source_workbook: Optional[str] = None,
    **extra: Any,
) -> Dict[str, Any]:
    record = {
        "category": category,
        "amount": amount,
        "unit": unit,
        "row_index_original": row_index_original,
        "source_section": source_section,
        "reporting_period": reporting_period,
        "source_file": source_file,
        "source_sheet": source_sheet,
        "source_workbook": source_workbook,
    }
    record.update(extra)
    return record


def append_parser_warning(diag: Dict[str, Any], message: str) -> None:
    diag.setdefault("warnings", [])
    if message not in diag["warnings"]:
        diag["warnings"].append(message)


def parse_vehicle_registry(df_raw: pd.DataFrame, diag: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    registry: Dict[str, Dict[str, Any]] = {}

    # Driver/assigned vehicle rows (e.g. Brad -> ID3 -> EF23 MZE)
    for row_idx in range(df_raw.shape[0]):
        driver_name = safe_text(df_raw.iat[row_idx, 19]) if df_raw.shape[1] > 19 else None
        vehicle_desc = safe_text(df_raw.iat[row_idx, 20]) if df_raw.shape[1] > 20 else None
        registration = safe_text(df_raw.iat[row_idx, 22]) if df_raw.shape[1] > 22 else None
        if driver_name and vehicle_desc and registration and driver_name.lower() not in {"business miles", "hsfg vehicles"}:
            vehicle_class = classify_vehicle_class(f"{vehicle_desc} {registration}")
            registry[registration] = {
                "vehicle_registration": registration,
                "raw_vehicle": vehicle_desc,
                "vehicle_class": vehicle_class,
                "driver_name": driver_name,
                "source": "driver_mapping",
            }

    # Fleet registry rows
    for row_idx in range(df_raw.shape[0]):
        registration = safe_text(df_raw.iat[row_idx, 19]) if df_raw.shape[1] > 19 else None
        type_hint = safe_text(df_raw.iat[row_idx, 20]) if df_raw.shape[1] > 20 else None
        model = safe_text(df_raw.iat[row_idx, 21]) if df_raw.shape[1] > 21 else None
        if registration and model:
            if registration.lower() in {"business miles", "hsfg vehicles"}:
                continue
            vehicle_text = " ".join([v for v in [type_hint, model] if v])
            vehicle_class = classify_vehicle_class(vehicle_text)
            existing = registry.get(registration, {})
            registry[registration] = {
                "vehicle_registration": registration,
                "raw_vehicle": model,
                "vehicle_type_hint": type_hint,
                "vehicle_class": existing.get("vehicle_class") or vehicle_class,
                "driver_name": existing.get("driver_name"),
                "source": "fleet_registry",
            }

    diag["fleet_registry"] = list(registry.values())[:50]
    return registry


def parse_electricity_sections(
    df_raw: pd.DataFrame,
    sections: List[Tuple[int, int, str]],
    diag: Dict[str, Any],
    parser_options: Dict[str, Any],
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for title_row, title_col, title_text in sections:
        parsed_here = 0
        blank_run = 0
        for row_idx in range(title_row + 2, min(df_raw.shape[0], title_row + 40)):
            date_value = df_raw.iat[row_idx, title_col]
            if safe_text(date_value) and safe_text(date_value).lower() == "total":
                break
            if is_blank(date_value):
                blank_run += 1
                if parsed_here > 0 and blank_run >= 2:
                    break
                continue
            blank_run = 0
            if isinstance(date_value, str) and "gas charges" in date_value.lower():
                break
            if not looks_like_date(date_value):
                continue

            total_kwh = None
            candidate_offsets = [4, 3, 5]
            for offset in candidate_offsets:
                col_idx = title_col + offset
                if col_idx < df_raw.shape[1] and looks_like_number(df_raw.iat[row_idx, col_idx]):
                    total_kwh = safe_float(df_raw.iat[row_idx, col_idx])
                    break

            if total_kwh is None or total_kwh <= 0:
                continue

            month = parse_date(date_value)
            invoice_reference = safe_text(df_raw.iat[row_idx, title_col + 1]) if title_col + 1 < df_raw.shape[1] else None
            period_label = month.strftime("%Y-%m") if month is not None else None

            records.append(build_normalized_activity(
                category="electricity",
                amount=total_kwh,
                unit="kWh",
                row_index_original=row_idx + 1,
                source_section=title_text,
                reporting_period=period_label,
                source_sheet=parser_options.get("source_sheet"),
                source_workbook=parser_options.get("source_workbook"),
                item_name=parser_options.get("electricity_country", DEFAULT_COUNTRY),
                country=parser_options.get("electricity_country", DEFAULT_COUNTRY),
                invoice_reference=invoice_reference,
                period_start=serialise_timestamp(month),
                period_end=serialise_timestamp(month),
                notes="Parsed from workbook electricity block",
            ))
            parsed_here += 1

        diag.setdefault("sections_parsed", []).append({
            "section": title_text,
            "record_count": parsed_here,
            "calculation_path": "electricity_kwh",
        })
    return records


def parse_gas_sections(
    df_raw: pd.DataFrame,
    sections: List[Tuple[int, int, str]],
    diag: Dict[str, Any],
    parser_options: Dict[str, Any],
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    gas_unit = parser_options.get("gas_unit", "kWh (Gross CV)")
    for title_row, title_col, title_text in sections:
        parsed_here = 0
        blank_run = 0
        for row_idx in range(title_row + 2, min(df_raw.shape[0], title_row + 40)):
            period_value = df_raw.iat[row_idx, title_col]
            if safe_text(period_value) and safe_text(period_value).lower() == "total":
                break
            if is_blank(period_value):
                blank_run += 1
                if parsed_here > 0 and blank_run >= 2:
                    break
                continue
            blank_run = 0

            amount_col = title_col + 2
            if amount_col >= df_raw.shape[1]:
                continue
            amount = safe_float(df_raw.iat[row_idx, amount_col], default=-1)
            if amount <= 0:
                continue

            period_start, period_end = parse_period_bounds(period_value)
            if period_end is not None:
                reporting_period = period_end.strftime("%Y-%m")
            elif period_start is not None:
                reporting_period = period_start.strftime("%Y-%m")
            else:
                reporting_period = safe_text(period_value)
            invoice_reference = safe_text(df_raw.iat[row_idx, title_col + 1]) if title_col + 1 < df_raw.shape[1] else None

            records.append(build_normalized_activity(
                category="fuel",
                amount=amount,
                unit=gas_unit,
                row_index_original=row_idx + 1,
                source_section=title_text,
                reporting_period=reporting_period,
                source_sheet=parser_options.get("source_sheet"),
                source_workbook=parser_options.get("source_workbook"),
                fuel="Natural gas",
                item_name="Natural gas",
                invoice_reference=invoice_reference,
                period_start=serialise_timestamp(period_start),
                period_end=serialise_timestamp(period_end),
                notes="Parsed from workbook gas block",
            ))
            parsed_here += 1

        diag.setdefault("sections_parsed", []).append({
            "section": title_text,
            "record_count": parsed_here,
            "calculation_path": "gas_kwh",
        })
    return records


def parse_fuel_litres_sections(
    df_raw: pd.DataFrame,
    sections: List[Tuple[int, int, str]],
    diag: Dict[str, Any],
    parser_options: Dict[str, Any],
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    default_fuel_type = parser_options.get("fleet_fuel_type", "Diesel")
    include_fleet_fuel = bool(parser_options.get("include_fleet_fuel", True))
    for title_row, title_col, title_text in sections:
        parsed_here = 0
        for row_idx in range(title_row + 1, min(df_raw.shape[0], title_row + 40)):
            date_value = df_raw.iat[row_idx, title_col]
            if safe_text(date_value) and safe_text(date_value).lower() == "total":
                break
            if not looks_like_date(date_value):
                continue

            amount_col = title_col + 1
            if amount_col >= df_raw.shape[1]:
                continue
            litres = safe_float(df_raw.iat[row_idx, amount_col], default=-1)
            if litres <= 0:
                continue

            month = parse_date(date_value)
            period_label = month.strftime("%Y-%m") if month is not None else None

            activity = build_normalized_activity(
                category="liquid fuel",
                amount=litres,
                unit="litres",
                row_index_original=row_idx + 1,
                source_section=title_text,
                reporting_period=period_label,
                source_sheet=parser_options.get("source_sheet"),
                source_workbook=parser_options.get("source_workbook"),
                fuel=default_fuel_type,
                item_name=default_fuel_type,
                period_start=serialise_timestamp(month),
                period_end=serialise_timestamp(month),
                notes="Parsed from workbook fleet fuel block",
            )
            if include_fleet_fuel:
                records.append(activity)
                parsed_here += 1
            else:
                diag.setdefault("excluded_sections", []).append({
                    "section": title_text,
                    "row_index_original": row_idx + 1,
                    "reason": "Fleet fuel block detected but excluded because include_fleet_fuel=False.",
                })

        diag.setdefault("sections_parsed", []).append({
            "section": title_text,
            "record_count": parsed_here,
            "calculation_path": "liquid_fuel_litres" if include_fleet_fuel else "liquid_fuel_excluded",
        })
    return records


def parse_business_miles(
    df_raw: pd.DataFrame,
    sections: List[Tuple[int, int, str]],
    diag: Dict[str, Any],
    parser_options: Dict[str, Any],
    vehicle_registry: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    include_business_miles = bool(parser_options.get("include_business_mileage", False))
    include_when_fuel_present = bool(parser_options.get("include_business_mileage_when_fuel_present", False))

    # Infer if the workbook already contains direct fuel use.
    workbook_has_fuel = any(
        item.get("calculation_path") == "liquid_fuel_litres"
        for item in diag.get("sections_parsed", [])
    )

    for title_row, title_col, title_text in sections:
        driver_row = title_row
        value_row = min(title_row + 1, df_raw.shape[0] - 1)
        parsed_here = 0

        for col_idx in range(title_col + 1, min(df_raw.shape[1], title_col + 8)):
            driver_name = safe_text(df_raw.iat[driver_row, col_idx])
            miles = safe_float(df_raw.iat[value_row, col_idx], default=-1)
            if not driver_name or miles < 0:
                continue

            vehicle_record = None
            for item in vehicle_registry.values():
                if safe_text(item.get("driver_name")) and item["driver_name"].strip().lower() == driver_name.strip().lower():
                    vehicle_record = item
                    break

            activity = build_normalized_activity(
                category="business travel",
                amount=miles,
                unit="miles",
                row_index_original=value_row + 1,
                source_section=title_text,
                reporting_period=parser_options.get("default_business_miles_period"),
                source_sheet=parser_options.get("source_sheet"),
                source_workbook=parser_options.get("source_workbook"),
                item_name=driver_name,
                driver_name=driver_name,
                raw_vehicle=(vehicle_record or {}).get("raw_vehicle"),
                vehicle_class=(vehicle_record or {}).get("vehicle_class"),
                vehicle_registration=(vehicle_record or {}).get("vehicle_registration"),
                notes="Parsed from workbook business miles block",
            )

            vehicle_class = activity.get("vehicle_class")
            override_table = parser_options.get("vehicle_factor_overrides", {}) or {}
            override = override_table.get(vehicle_class)

            if override:
                activity["emission_factor_override"] = override.get("kg_co2e_per_unit")
                activity["factor_unit_override"] = override.get("unit", "vehicle_km")
                activity["factor_name_override"] = override.get("factor_name") or f"{vehicle_class} mileage factor"
                activity["factor_description_override"] = override.get("factor_description") or "Vehicle-specific factor supplied at runtime"
                activity["factor_quality_override"] = override.get("factor_quality", "runtime_override")
                activity["factor_year_override"] = override.get("factor_year", DEFAULT_FACTOR_YEAR)
                activity["data_source_override"] = override.get("data_source", "Runtime vehicle factor override")
                activity["scope_override"] = override.get("scope", "Scope 3")

            should_exclude = (not include_business_miles) or (workbook_has_fuel and not include_when_fuel_present)
            if should_exclude:
                diag.setdefault("excluded_sections", []).append({
                    "section": title_text,
                    "driver_name": driver_name,
                    "reason": "Business mileage retained for mapping/transparency but excluded from totals to avoid double counting with direct fleet fuel or because include_business_mileage=False.",
                    "vehicle_class": vehicle_class,
                    "vehicle_registration": activity.get("vehicle_registration"),
                })
            else:
                records.append(activity)
                parsed_here += 1

        diag.setdefault("sections_parsed", []).append({
            "section": title_text,
            "record_count": parsed_here,
            "calculation_path": "business_mileage" if include_business_miles else "business_mileage_excluded",
        })

    return records


def parse_ev_charging(
    df_raw: pd.DataFrame,
    sections: List[Tuple[int, int, str]],
    diag: Dict[str, Any],
    parser_options: Dict[str, Any],
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    include_ev_charging = bool(parser_options.get("include_ev_charging_submeter", False))

    for title_row, title_col, title_text in sections:
        header_row = min(title_row + 2, df_raw.shape[0] - 1)
        totals_preview = []
        parsed_here = 0

        for col_idx in range(title_col + 1, min(df_raw.shape[1], title_col + 8)):
            label = safe_text(df_raw.iat[header_row, col_idx])
            if not label:
                continue

            total = 0.0
            for row_idx in range(header_row + 2, min(df_raw.shape[0], header_row + 20)):
                date_value = df_raw.iat[row_idx, title_col]
                if not looks_like_date(date_value):
                    break
                total += safe_float(df_raw.iat[row_idx, col_idx], default=0.0)

            totals_preview.append({"label": label, "kwh": round(total, 3)})

            if include_ev_charging and total > 0:
                records.append(build_normalized_activity(
                    category="electricity",
                    amount=total,
                    unit="kWh",
                    row_index_original=header_row + 1,
                    source_section=f"{title_text} / {label}",
                    reporting_period=parser_options.get("default_ev_charging_period"),
                    source_sheet=parser_options.get("source_sheet"),
                    source_workbook=parser_options.get("source_workbook"),
                    item_name=f"EV charging - {label}",
                    country=parser_options.get("electricity_country", DEFAULT_COUNTRY),
                    notes="Parsed from EV charging sub-meter block",
                ))
                parsed_here += 1

        diag.setdefault("ev_charging_preview", totals_preview[:20])
        diag.setdefault("sections_parsed", []).append({
            "section": title_text,
            "record_count": parsed_here,
            "calculation_path": "ev_charging_submeter" if include_ev_charging else "ev_charging_excluded",
        })

        if not include_ev_charging and totals_preview:
            append_parser_warning(
                diag,
                "EV charging block detected. It has been excluded from totals by default to avoid double counting against main electricity unless include_ev_charging_submeter=True."
            )

    return records


def parse_energy_workbook_records(records: List[Dict[str, Any]], parser_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    parser_options = parser_options or {}
    df_raw = records_to_dataframe(records)
    diag: Dict[str, Any] = {
        "parser_used": "workbook_block_parser",
        "input_shape": {"rows": int(df_raw.shape[0]), "cols": int(df_raw.shape[1])},
        "sections_detected": [],
        "sections_parsed": [],
        "excluded_sections": [],
        "warnings": [],
    }

    if df_raw.empty:
        return {"activities": [], "parser_diagnostics": diag}

    sections = detect_workbook_sections(df_raw)
    diag["sections_detected"] = {
        key: [{"row": row + 1, "col": col + 1, "label": text} for row, col, text in value]
        for key, value in sections.items()
        if value
    }

    vehicle_registry = parse_vehicle_registry(df_raw, diag)

    activities: List[Dict[str, Any]] = []
    activities.extend(parse_electricity_sections(df_raw, sections.get("electricity", []), diag, parser_options))
    activities.extend(parse_gas_sections(df_raw, sections.get("gas", []), diag, parser_options))
    activities.extend(parse_fuel_litres_sections(df_raw, sections.get("fuel_litres", []), diag, parser_options))
    activities.extend(parse_ev_charging(df_raw, sections.get("ev_charging", []), diag, parser_options))
    activities.extend(parse_business_miles(df_raw, sections.get("business_miles", []), diag, parser_options, vehicle_registry))

    if sections.get("adblue"):
        append_parser_warning(
            diag,
            "AdBlue section detected, but no default AdBlue factor is configured in this engine. Provide a runtime factor if you want it calculated."
        )

    if not activities:
        append_parser_warning(
            diag,
            "No calculable activities were extracted from the workbook parser. Check section labels, parser options, or upload a mapped template."
        )

    diag["extracted_activity_count"] = len(activities)
    diag["fleet_registry_count"] = len(vehicle_registry)

    return {"activities": activities, "parser_diagnostics": diag}


def prepare_activities_for_engine(activity_list: List[Dict[str, Any]], parser_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    parser_options = parser_options or {}
    if not activity_list:
        return {
            "activities": [],
            "parser_diagnostics": {
                "parser_used": "none",
                "warnings": ["No activities were supplied."],
            }
        }

    df_probe = pd.DataFrame(activity_list)
    if looks_like_canonical_activity_df(df_probe):
        canonical_df = preprocess_uploaded_dataframe(df_probe)
        return {
            "activities": canonical_df.to_dict(orient="records"),
            "parser_diagnostics": {
                "parser_used": "canonical_records",
                "warnings": [],
                "input_shape": {"rows": int(canonical_df.shape[0]), "cols": int(canonical_df.shape[1])},
                "extracted_activity_count": int(canonical_df.shape[0]),
            },
        }

    return parse_energy_workbook_records(activity_list, parser_options=parser_options)


# =========================
# DISPATCHER
# =========================
def calculate_emissions(activity: Dict[str, Any]) -> pd.DataFrame:
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
    extra_fields = {field: activity.get(field, None) for field in METADATA_FIELDS}

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
            mapping_confidence=mapping_confidence,
            extra_fields=extra_fields,
        )

    if canonical_category == "liquid fuel":
        fuel = activity.get("fuel") or activity.get("item_name") or "Diesel"
        return calculate_liquid_fuel_emissions_unified(
            df=df_liquid_fuels_clean,
            fuel=fuel,
            unit=unit,
            amount=amount,
            factor_year=activity.get("factor_year", DEFAULT_FACTOR_YEAR),
            original_category=original_category,
            mapping_source=mapping_source,
            mapping_confidence=mapping_confidence,
            extra_fields=extra_fields,
        )

    if canonical_category == "electricity":
        country = activity.get("country") or activity.get("item_name") or DEFAULT_COUNTRY
        unit = unit or "kWh"
        return calculate_electricity_emissions_unified(
            df=df_electricity_clean,
            country=country,
            unit=unit,
            amount=amount,
            year=activity.get("year", DEFAULT_FACTOR_YEAR),
            original_category=original_category,
            mapping_source=mapping_source,
            mapping_confidence=mapping_confidence,
            extra_fields=extra_fields,
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
            mapping_confidence=mapping_confidence,
            extra_fields=extra_fields,
        )

    if canonical_category == "business travel":
        if activity.get("emission_factor_override") is not None:
            return calculate_business_travel_with_override(activity, mapping)

        travel_type = infer_business_travel_type(activity)
        return calculate_business_travel_emissions_unified(
            df=df_business_travel_clean,
            travel_type=travel_type,
            unit=unit,
            amount=amount,
            factor_year=activity.get("factor_year", DEFAULT_FACTOR_YEAR),
            original_category=original_category,
            mapping_source=mapping_source,
            mapping_confidence=mapping_confidence,
            extra_fields=extra_fields,
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
            mapping_confidence=mapping_confidence,
            extra_fields=extra_fields,
        )

    raise ValueError(
        f"Unsupported category: '{original_category}'. {SUPPORTED_CATEGORY_HINT}"
    )


# =========================
# ERROR GROUPING / QUALITY
# =========================
def infer_issue_type_from_message(message: str) -> str:
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
    if "no matching factor found" in msg or "no matching electricity factor found" in msg or "no matching water factor found" in msg or "no matching liquid-fuel factor found" in msg:
        return "Factor matching failure"
    if "year must be" in msg or "year is out of valid range" in msg:
        return "Invalid year"
    if "required" in msg:
        return "Missing required field"
    return "Other validation issue"


def infer_severity(issue_type: str, count: int = 1) -> str:
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


def infer_issue_guidance(issue_type: str) -> str:
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


def group_errors(errors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not errors:
        return []

    grouped: Dict[str, Dict[str, Any]] = {}

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


def build_error_summary(errors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped = group_errors(errors)
    return [{"error": item["title"], "count": item["count"]} for item in grouped]


def build_errors_preview(errors: List[Dict[str, Any]], max_items: int = 10) -> List[Dict[str, Any]]:
    return errors[:max_items]


def calculate_confidence_score(report_df: pd.DataFrame, data_quality: Dict[str, Any], parser_diagnostics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    total_rows = int(data_quality.get("total_rows", 0) or 0)
    coverage_percent = float(data_quality.get("coverage_percent", 0.0) or 0.0)
    inferred_rows = int(data_quality.get("inferred_category_rows", 0) or 0)
    estimated_rows = int(data_quality.get("estimated_rows", 0) or 0)
    placeholder_rows = int(data_quality.get("starter_factor_rows", 0) or 0)
    parser_warnings = len((parser_diagnostics or {}).get("warnings", []))
    excluded_sections = len((parser_diagnostics or {}).get("excluded_sections", []))

    if total_rows <= 0:
        return {
            "score": 0,
            "label": "Low",
            "coverage_percent": 0.0,
            "notes": ["No valid rows were available for calculation."]
        }

    if coverage_percent <= 0:
        return {
            "score": 0,
            "label": "Low",
            "coverage_percent": coverage_percent,
            "notes": [
                "No rows were processed successfully, so the result should not be used externally.",
                "Fix parsing, validation, or factor-matching issues before generating a client-facing report.",
            ],
        }

    score = 100.0
    score -= max(0.0, 100.0 - coverage_percent) * 0.65
    score -= (inferred_rows / total_rows) * 15.0
    score -= (estimated_rows / total_rows) * 20.0
    score -= (placeholder_rows / total_rows) * 20.0
    score -= min(parser_warnings * 3.0, 12.0)
    score -= min(excluded_sections * 1.0, 8.0)

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
        notes.append("Some rows were excluded due to validation, parser, or factor-matching issues.")

    if inferred_rows > 0:
        notes.append(f"{inferred_rows} row(s) relied on inferred category mapping.")
    if estimated_rows > 0:
        notes.append(f"{estimated_rows} row(s) used estimated or non-exact treatment.")
    if placeholder_rows > 0:
        notes.append(f"{placeholder_rows} row(s) used starter placeholder factor tables that should be replaced with your official DEFRA dataset.")
    if parser_warnings > 0:
        notes.append(f"{parser_warnings} parser warning(s) were raised and should be reviewed.")
    if excluded_sections > 0:
        notes.append(f"{excluded_sections} section or row(s) were excluded from totals to avoid double counting or unsupported assumptions.")

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
def calculate_emissions_batch_safe(activity_list: List[Dict[str, Any]], parser_diagnostics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
                "source_section": activity.get("source_section"),
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

    confidence = calculate_confidence_score(final_report, data_quality, parser_diagnostics=parser_diagnostics)

    return {
        "report": final_report,
        "total_kgCO2e": total_kg,
        "total_tCO2e": total_t,
        "errors": errors,
        "data_quality": data_quality,
        "unmapped_categories": unmapped_summary,
        "confidence": confidence,
        "parser_diagnostics": parser_diagnostics or {},
    }


# =========================
# SUMMARIES
# =========================
def summarize_report_by_scope_and_category(report_df: pd.DataFrame) -> pd.DataFrame:
    if report_df.empty:
        return pd.DataFrame(columns=["scope", "category", "emissions_kgCO2e", "emissions_tCO2e"])

    return (
        report_df
        .groupby(["scope", "category"], as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
        .sum()
        .sort_values(["scope", "category"])
        .reset_index(drop=True)
    )


def summarize_report_by_scope(report_df: pd.DataFrame) -> pd.DataFrame:
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


def summarize_top_categories(report_df: pd.DataFrame, top_n: int = 5) -> List[Dict[str, Any]]:
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


def summarize_top_sites(report_df: pd.DataFrame, top_n: int = 5) -> List[Dict[str, Any]]:
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


def summarize_factor_quality(report_df: pd.DataFrame) -> List[Dict[str, Any]]:
    if report_df.empty or "factor_quality" not in report_df.columns:
        return []

    df = report_df["factor_quality"].fillna("unknown").value_counts().reset_index()
    df.columns = ["factor_quality", "count"]
    return df.to_dict(orient="records")


def build_factor_transparency(report_df: pd.DataFrame) -> List[Dict[str, Any]]:
    if report_df.empty:
        return []

    cols = [
        "category",
        "scope",
        "item_name",
        "vehicle_class",
        "vehicle_registration",
        "driver_name",
        "factor_name",
        "factor_description",
        "emission_factor_kgCO2e_per_unit",
        "normalized_unit",
        "factor_year",
        "data_source",
        "factor_quality",
        "calculation_basis",
        "source_section",
        "reporting_period"
    ]

    available_cols = [c for c in cols if c in report_df.columns]
    factor_df = report_df[available_cols].drop_duplicates().reset_index(drop=True)

    factor_df = factor_df.sort_values(
        by=[c for c in ["category", "item_name", "factor_name"] if c in factor_df.columns]
    ).reset_index(drop=True)

    return factor_df.to_dict(orient="records")


# =========================
# ANALYTICS / INSIGHTS
# =========================
def build_analytics(report_df: pd.DataFrame) -> Dict[str, Any]:
    analytics: Dict[str, Any] = {}

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


def build_plain_language_takeaways(report_df: pd.DataFrame, batch_result: Dict[str, Any], summary_scope: pd.DataFrame, analytics: Dict[str, Any]) -> List[str]:
    takeaways: List[str] = []

    total_t = round(float(batch_result.get("total_tCO2e", 0.0)), 2)
    total_kg = round(float(batch_result.get("total_kgCO2e", 0.0)), 2)
    data_quality = batch_result.get("data_quality", {})
    confidence = batch_result.get("confidence", {})
    parser_diagnostics = batch_result.get("parser_diagnostics", {})

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

    coverage = data_quality.get("coverage_percent", 0.0)
    if coverage > 0:
        takeaways.append(
            f"Data processing coverage is {coverage}%, which indicates how much of the uploaded dataset was successfully included in the calculation."
        )

    if parser_diagnostics.get("parser_used") == "workbook_block_parser":
        takeaways.append(
            f"The workbook parser extracted {parser_diagnostics.get('extracted_activity_count', 0)} calculable activity rows from a semi-structured source file."
        )

    if confidence:
        takeaways.append(
            f"The current confidence score is {confidence.get('score', 0)}/100, rated {confidence.get('label', 'Low')}."
        )

    return takeaways[:6]


def build_actionable_insights(report_df: pd.DataFrame, batch_result: Dict[str, Any], summary_scope: pd.DataFrame, analytics: Dict[str, Any]) -> List[Dict[str, Any]]:
    insights: List[Dict[str, Any]] = []
    data_quality = batch_result.get("data_quality", {})
    confidence = batch_result.get("confidence", {})
    unmapped_categories = batch_result.get("unmapped_categories", [])
    parser_diagnostics = batch_result.get("parser_diagnostics", {})

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

    excluded_sections = parser_diagnostics.get("excluded_sections", [])
    if excluded_sections:
        insights.append({
            "title": "Double-counting safeguards applied",
            "body": (
                f"{len(excluded_sections)} mileage or sub-meter item(s) were excluded automatically to avoid double counting "
                f"against direct fuel or main electricity totals. Review the parser diagnostics if you want different inclusion rules."
            )
        })

    if confidence and confidence.get("score", 0) < 60:
        insights.append({
            "title": "Low-confidence result",
            "body": (
                f"The current confidence score is {confidence.get('score', 0)}/100. "
                f"Input cleanup, factor completion, and parser review should be prioritised before relying on this result for external communication."
            )
        })

    return insights[:8]


def build_methodology_summary(batch_result: Dict[str, Any]) -> Dict[str, Any]:
    data_quality = batch_result.get("data_quality", {})
    starter_rows = int(data_quality.get("starter_factor_rows", 0) or 0)
    estimated_rows = int(data_quality.get("estimated_rows", 0) or 0)
    parser_diagnostics = batch_result.get("parser_diagnostics", {})

    summary = {
        "framework": "GHG Protocol aligned reporting structure using uploaded activity data and emissions factors.",
        "factor_basis": "Fuel, electricity, water, and direct liquid-fuel combustion use factor tables inside this engine. Business travel and waste currently rely on starter tables unless replaced by your official DEFRA source tables or runtime overrides.",
        "calculation_logic": "Each valid input row is mapped to a canonical emissions category, matched to an emissions factor, multiplied by the activity quantity, and aggregated into total CO2e outputs. Semi-structured workbooks can first be normalized through the built-in block parser.",
        "assumptions": [],
        "interpretation_notes": []
    }

    if parser_diagnostics.get("parser_used") == "workbook_block_parser":
        summary["assumptions"].append(
            "The uploaded workbook was semi-structured, so a parser was used to extract electricity, gas, and fuel blocks into canonical activity rows."
        )

    if estimated_rows > 0:
        summary["assumptions"].append(
            "Some rows were estimated based on inferred category mapping or non-exact treatment."
        )

    if starter_rows > 0:
        summary["assumptions"].append(
            "Some Scope 3 rows used starter placeholder factor tables and should be replaced with the official DEFRA factors before final reporting."
        )

    if parser_diagnostics.get("excluded_sections"):
        summary["assumptions"].append(
            "Some sections were excluded intentionally to avoid double counting or unsupported assumptions."
        )

    summary["interpretation_notes"].append(
        "Results should be read alongside the data quality, parser diagnostics, and confidence sections, especially when rows were excluded or estimated."
    )
    summary["interpretation_notes"].append(
        "Unmapped categories, factor-matching failures, and parser warnings should be corrected before external reporting."
    )

    return summary


# =========================
# RESPONSE BUILDERS
# =========================
def build_final_response(batch_result: Dict[str, Any]) -> Dict[str, Any]:
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
        "parser_diagnostics": batch_result.get("parser_diagnostics", {}),
        "errors": batch_result["errors"],
        "errors_preview": errors_preview,
        "error_summary": error_summary,
        "issue_groups": error_groups,
        "unmapped_categories": batch_result.get("unmapped_categories", []),
        "factor_transparency": factor_transparency,
        "factor_quality_summary": factor_quality_summary,
        "methodology_summary": methodology_summary
    }


def make_json_safe(obj: Any) -> Any:
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
    if isinstance(obj, pd.DataFrame):
        return make_json_safe(obj.to_dict(orient="records"))
    return obj


def run_emissions_engine(request_json: Dict[str, Any]) -> Dict[str, Any]:
    activities = request_json.get("activities", [])
    parser_options = request_json.get("parser_options", {}) or {}

    prepared = prepare_activities_for_engine(activities, parser_options=parser_options)
    normalized_activities = prepared.get("activities", [])
    parser_diagnostics = prepared.get("parser_diagnostics", {})

    batch_result = calculate_emissions_batch_safe(
        normalized_activities,
        parser_diagnostics=parser_diagnostics,
    )
    final_response = build_final_response(batch_result)
    return make_json_safe(final_response)

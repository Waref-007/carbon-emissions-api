"""
engine.py - Carbon emissions calculation engine

Updated audit-ready foundation for:
- Standard activity-row inputs
- Transformed client energy-report rows
- DEFRA 2023/2024/2025 factor-year handling
- Scope 1/2/3 categories
- Fuel in kWh, cubic metres, tonnes, litres
- Electricity in kWh
- Water in cubic metres / million litres
- Business travel in passenger km, vehicle km, miles, room nights, GBP
- Waste in kg / tonnes
- Stronger audit trail fields per line item

Important:
This file includes a small built-in starter factor library so your app keeps running.
For audit-grade use, replace BUILTIN_FACTOR_ROWS with factors loaded from official
UK Government / DEFRA flat files via a factor_loader module.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


# ============================================================
# VERSIONING
# ============================================================
ENGINE_VERSION = "2.0.0"
DEFAULT_FACTOR_YEAR = 2025
SUPPORTED_FACTOR_YEARS = {2023, 2024, 2025}

SUPPORTED_CATEGORY_HINT = (
    "Supported / mappable categories include: fuel, electricity, water, "
    "business travel, travel, hotel, waste, diesel, petrol, gas, EV charging."
)

METADATA_FIELDS = [
    "record_id",
    "client_name",
    "site_name",
    "reporting_period",
    "period_start",
    "period_end",
    "reporting_year",
    "notes",
    "source_file",
    "source_section",
    "row_index_original",
    "raw_column",
    "raw_row_label",
]


# ============================================================
# BUILT-IN FACTORS
# ============================================================
# These are deliberately structured as an audit factor table.
# Replace / extend this table with official flat-file ingestion.
# Values below preserve your current factors where provided, and add starter
# rows for litres/miles workflows. Treat starter_placeholder rows as not final.

BUILTIN_FACTOR_ROWS: List[Dict[str, Any]] = [
    # -------------------------
    # Natural gas - from your current model
    # -------------------------
    {
        "factor_id": "fuel-natural-gas-gross-kwh-2025",
        "factor_year": 2025,
        "category": "fuel",
        "display_category": "Fuel",
        "scope": "Scope 1",
        "activity_group": "Gaseous fuels",
        "activity": "Natural gas",
        "item_name": "Natural gas",
        "unit": "kWh (Gross CV)",
        "kgCO2e_per_unit": 0.18296,
        "source_name": "DEFRA 2025 - replace with official flat-file row reference",
        "source_file": "built_in_engine_table",
        "source_sheet": "fuel",
        "source_row": None,
        "factor_quality": "official_pending_traceability",
        "factor_description": "Natural gas combustion factor, gross CV",
    },
    {
        "factor_id": "fuel-natural-gas-net-kwh-2025",
        "factor_year": 2025,
        "category": "fuel",
        "display_category": "Fuel",
        "scope": "Scope 1",
        "activity_group": "Gaseous fuels",
        "activity": "Natural gas",
        "item_name": "Natural gas",
        "unit": "kWh (Net CV)",
        "kgCO2e_per_unit": 0.20270,
        "source_name": "DEFRA 2025 - replace with official flat-file row reference",
        "source_file": "built_in_engine_table",
        "source_sheet": "fuel",
        "source_row": None,
        "factor_quality": "official_pending_traceability",
        "factor_description": "Natural gas combustion factor, net CV",
    },
    {
        "factor_id": "fuel-natural-gas-m3-2025",
        "factor_year": 2025,
        "category": "fuel",
        "display_category": "Fuel",
        "scope": "Scope 1",
        "activity_group": "Gaseous fuels",
        "activity": "Natural gas",
        "item_name": "Natural gas",
        "unit": "cubic metres",
        "kgCO2e_per_unit": 2.06672,
        "source_name": "DEFRA 2025 - replace with official flat-file row reference",
        "source_file": "built_in_engine_table",
        "source_sheet": "fuel",
        "source_row": None,
        "factor_quality": "official_pending_traceability",
        "factor_description": "Natural gas combustion factor, cubic metres",
    },
    {
        "factor_id": "fuel-natural-gas-tonnes-2025",
        "factor_year": 2025,
        "category": "fuel",
        "display_category": "Fuel",
        "scope": "Scope 1",
        "activity_group": "Gaseous fuels",
        "activity": "Natural gas",
        "item_name": "Natural gas",
        "unit": "tonnes",
        "kgCO2e_per_unit": 2575.46441,
        "source_name": "DEFRA 2025 - replace with official flat-file row reference",
        "source_file": "built_in_engine_table",
        "source_sheet": "fuel",
        "source_row": None,
        "factor_quality": "official_pending_traceability",
        "factor_description": "Natural gas combustion factor, tonnes",
    },

    # -------------------------
    # Electricity - your current model
    # Add 2023/2024 placeholders so matching works while factor loader is built.
    # Replace with official DEFRA flat-file values before audit use.
    # -------------------------
    {
        "factor_id": "electricity-uk-kwh-2023-placeholder",
        "factor_year": 2023,
        "category": "electricity",
        "display_category": "Electricity",
        "scope": "Scope 2",
        "activity_group": "Electricity generated",
        "activity": "Electricity: UK",
        "item_name": "Electricity: UK",
        "unit": "kWh",
        "kgCO2e_per_unit": 0.20705,
        "source_name": "Starter value - replace with official DEFRA 2023 flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "electricity",
        "source_row": None,
        "factor_quality": "starter_placeholder",
        "factor_description": "UK location-based electricity factor placeholder",
    },
    {
        "factor_id": "electricity-uk-kwh-2024-placeholder",
        "factor_year": 2024,
        "category": "electricity",
        "display_category": "Electricity",
        "scope": "Scope 2",
        "activity_group": "Electricity generated",
        "activity": "Electricity: UK",
        "item_name": "Electricity: UK",
        "unit": "kWh",
        "kgCO2e_per_unit": 0.20705,
        "source_name": "Starter value - replace with official DEFRA 2024 flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "electricity",
        "source_row": None,
        "factor_quality": "starter_placeholder",
        "factor_description": "UK location-based electricity factor placeholder",
    },
    {
        "factor_id": "electricity-uk-kwh-2025",
        "factor_year": 2025,
        "category": "electricity",
        "display_category": "Electricity",
        "scope": "Scope 2",
        "activity_group": "Electricity generated",
        "activity": "Electricity: UK",
        "item_name": "Electricity: UK",
        "unit": "kWh",
        "kgCO2e_per_unit": 0.177,
        "source_name": "DEFRA 2025 - replace with official flat-file row reference",
        "source_file": "built_in_engine_table",
        "source_sheet": "electricity",
        "source_row": None,
        "factor_quality": "official_pending_traceability",
        "factor_description": "UK location-based electricity factor",
    },

    # -------------------------
    # Water - your current model
    # -------------------------
    {
        "factor_id": "water-supply-m3-2025",
        "factor_year": 2025,
        "category": "water",
        "display_category": "Water",
        "scope": "Scope 3",
        "activity_group": "Water supply",
        "activity": "Water supply",
        "item_name": "Water supply",
        "unit": "cubic metres",
        "kgCO2e_per_unit": 0.1913,
        "source_name": "DEFRA 2025 - replace with official flat-file row reference",
        "source_file": "built_in_engine_table",
        "source_sheet": "water",
        "source_row": None,
        "factor_quality": "official_pending_traceability",
        "factor_description": "Water supply factor",
    },
    {
        "factor_id": "water-supply-million-litres-2025",
        "factor_year": 2025,
        "category": "water",
        "display_category": "Water",
        "scope": "Scope 3",
        "activity_group": "Water supply",
        "activity": "Water supply",
        "item_name": "Water supply",
        "unit": "million litres",
        "kgCO2e_per_unit": 191.30156,
        "source_name": "DEFRA 2025 - replace with official flat-file row reference",
        "source_file": "built_in_engine_table",
        "source_sheet": "water",
        "source_row": None,
        "factor_quality": "official_pending_traceability",
        "factor_description": "Water supply factor",
    },

    # -------------------------
    # Liquid fuels - starter placeholders until official flat-file is loaded
    # -------------------------
    {
        "factor_id": "fuel-diesel-litres-placeholder",
        "factor_year": 2025,
        "category": "fuel",
        "display_category": "Fuel",
        "scope": "Scope 1",
        "activity_group": "Liquid fuels",
        "activity": "Diesel",
        "item_name": "Diesel",
        "unit": "litres",
        "kgCO2e_per_unit": 2.70,
        "source_name": "Starter liquid fuel factor - replace with official DEFRA flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "liquid fuels",
        "source_row": None,
        "factor_quality": "starter_placeholder",
        "factor_description": "Diesel litres starter factor",
    },
    {
        "factor_id": "fuel-petrol-litres-placeholder",
        "factor_year": 2025,
        "category": "fuel",
        "display_category": "Fuel",
        "scope": "Scope 1",
        "activity_group": "Liquid fuels",
        "activity": "Petrol",
        "item_name": "Petrol",
        "unit": "litres",
        "kgCO2e_per_unit": 2.31,
        "source_name": "Starter liquid fuel factor - replace with official DEFRA flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "liquid fuels",
        "source_row": None,
        "factor_quality": "starter_placeholder",
        "factor_description": "Petrol litres starter factor",
    },
    {
        "factor_id": "fuel-adblue-litres-placeholder",
        "factor_year": 2025,
        "category": "fuel",
        "display_category": "Fuel",
        "scope": "Scope 3",
        "activity_group": "Vehicle consumables",
        "activity": "AdBlue",
        "item_name": "AdBlue",
        "unit": "litres",
        "kgCO2e_per_unit": 0.0,
        "source_name": "No official factor configured - treat as disclosure only until confirmed",
        "source_file": "built_in_engine_table",
        "source_sheet": "vehicle consumables",
        "source_row": None,
        "factor_quality": "disclosure_only_placeholder",
        "factor_description": "AdBlue recorded but not emissions-calculated until official treatment is confirmed",
    },

    # -------------------------
    # Business travel / mileage - placeholders
    # -------------------------
    {
        "factor_id": "business-travel-hotel-room-nights-placeholder",
        "factor_year": 2025,
        "category": "business travel",
        "display_category": "Business Travel",
        "scope": "Scope 3",
        "activity_group": "Business travel",
        "activity": "Hotel stay",
        "item_name": "Hotel stay",
        "unit": "room_nights",
        "kgCO2e_per_unit": 10.40,
        "source_name": "Starter Scope 3 factor - replace with official DEFRA flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "business travel",
        "source_row": None,
        "factor_quality": "starter_placeholder",
        "factor_description": "Hotel stay starter factor",
    },
    {
        "factor_id": "business-travel-flight-passenger-km-placeholder",
        "factor_year": 2025,
        "category": "business travel",
        "display_category": "Business Travel",
        "scope": "Scope 3",
        "activity_group": "Business travel",
        "activity": "Flight",
        "item_name": "Flight",
        "unit": "passenger_km",
        "kgCO2e_per_unit": 0.146,
        "source_name": "Starter Scope 3 factor - replace with official DEFRA flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "business travel",
        "source_row": None,
        "factor_quality": "starter_placeholder",
        "factor_description": "Flight starter factor",
    },
    {
        "factor_id": "business-travel-rail-passenger-km-placeholder",
        "factor_year": 2025,
        "category": "business travel",
        "display_category": "Business Travel",
        "scope": "Scope 3",
        "activity_group": "Business travel",
        "activity": "Rail",
        "item_name": "Rail",
        "unit": "passenger_km",
        "kgCO2e_per_unit": 0.035,
        "source_name": "Starter Scope 3 factor - replace with official DEFRA flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "business travel",
        "source_row": None,
        "factor_quality": "starter_placeholder",
        "factor_description": "Rail starter factor",
    },
    {
        "factor_id": "business-travel-car-vehicle-km-placeholder",
        "factor_year": 2025,
        "category": "business travel",
        "display_category": "Business Travel",
        "scope": "Scope 3",
        "activity_group": "Business travel",
        "activity": "Taxi / car",
        "item_name": "Taxi / car",
        "unit": "vehicle_km",
        "kgCO2e_per_unit": 0.170,
        "source_name": "Starter Scope 3 factor - replace with official DEFRA flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "business travel",
        "source_row": None,
        "factor_quality": "starter_placeholder",
        "factor_description": "Generic car / taxi vehicle-km starter factor",
    },
    {
        "factor_id": "business-travel-gbp-placeholder",
        "factor_year": 2025,
        "category": "business travel",
        "display_category": "Business Travel",
        "scope": "Scope 3",
        "activity_group": "Business travel",
        "activity": "Generic travel spend",
        "item_name": "Generic travel spend",
        "unit": "gbp",
        "kgCO2e_per_unit": 0.28,
        "source_name": "Starter Scope 3 factor - replace with official DEFRA flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "business travel",
        "source_row": None,
        "factor_quality": "starter_placeholder",
        "factor_description": "Generic travel spend starter factor",
    },

    # -------------------------
    # Waste - placeholders
    # -------------------------
    {
        "factor_id": "waste-general-tonnes-placeholder",
        "factor_year": 2025,
        "category": "waste",
        "display_category": "Waste",
        "scope": "Scope 3",
        "activity_group": "Waste generated",
        "activity": "General waste",
        "item_name": "General waste",
        "unit": "tonnes",
        "kgCO2e_per_unit": 250.0,
        "source_name": "Starter Scope 3 factor - replace with official DEFRA flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "waste",
        "source_row": None,
        "factor_quality": "starter_placeholder",
        "factor_description": "General waste starter factor",
    },
    {
        "factor_id": "waste-landfill-tonnes-placeholder",
        "factor_year": 2025,
        "category": "waste",
        "display_category": "Waste",
        "scope": "Scope 3",
        "activity_group": "Waste generated",
        "activity": "Landfill waste",
        "item_name": "Landfill waste",
        "unit": "tonnes",
        "kgCO2e_per_unit": 458.0,
        "source_name": "Starter Scope 3 factor - replace with official DEFRA flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "waste",
        "source_row": None,
        "factor_quality": "starter_placeholder",
        "factor_description": "Landfill waste starter factor",
    },
    {
        "factor_id": "waste-recycling-tonnes-placeholder",
        "factor_year": 2025,
        "category": "waste",
        "display_category": "Waste",
        "scope": "Scope 3",
        "activity_group": "Waste generated",
        "activity": "Recycling",
        "item_name": "Recycling",
        "unit": "tonnes",
        "kgCO2e_per_unit": 21.0,
        "source_name": "Starter Scope 3 factor - replace with official DEFRA flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "waste",
        "source_row": None,
        "factor_quality": "starter_placeholder",
        "factor_description": "Recycling starter factor",
    },
    {
        "factor_id": "waste-incineration-tonnes-placeholder",
        "factor_year": 2025,
        "category": "waste",
        "display_category": "Waste",
        "scope": "Scope 3",
        "activity_group": "Waste generated",
        "activity": "Incineration",
        "item_name": "Incineration",
        "unit": "tonnes",
        "kgCO2e_per_unit": 27.0,
        "source_name": "Starter Scope 3 factor - replace with official DEFRA flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "waste",
        "source_row": None,
        "factor_quality": "starter_placeholder",
        "factor_description": "Incineration starter factor",
    },
]

# Expand kg waste rows from tonne factors
for _row in list(BUILTIN_FACTOR_ROWS):
    if _row.get("category") == "waste" and _row.get("unit") == "tonnes":
        kg_row = dict(_row)
        kg_row["factor_id"] = kg_row["factor_id"].replace("tonnes", "kg")
        kg_row["unit"] = "kg"
        kg_row["kgCO2e_per_unit"] = float(kg_row["kgCO2e_per_unit"]) / 1000.0
        BUILTIN_FACTOR_ROWS.append(kg_row)

FACTOR_TABLE = pd.DataFrame(BUILTIN_FACTOR_ROWS)


# ============================================================
# NORMALISATION DICTIONARIES
# ============================================================
CATEGORY_ALIASES = {
    "fuel": "fuel",
    "gas": "fuel",
    "natural gas": "fuel",
    "diesel": "fuel",
    "petrol": "fuel",
    "fuel litres": "fuel",
    "adblue": "fuel",
    "ad blue": "fuel",
    "electricity": "electricity",
    "power": "electricity",
    "ev charging": "electricity",
    "ev charger": "electricity",
    "ev electricity": "electricity",
    "water": "water",
    "water supply": "water",
    "travel": "business travel",
    "business travel": "business travel",
    "business miles": "business travel",
    "mileage": "business travel",
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

ITEM_ALIASES = {
    "natural gas": "Natural gas",
    "gas": "Natural gas",
    "electricity": "Electricity: UK",
    "electricity: uk": "Electricity: UK",
    "uk electricity": "Electricity: UK",
    "ev charging": "Electricity: UK",
    "ev charger": "Electricity: UK",
    "diesel": "Diesel",
    "diesel litres": "Diesel",
    "petrol": "Petrol",
    "petrol litres": "Petrol",
    "adblue": "AdBlue",
    "ad blue": "AdBlue",
    "water": "Water supply",
    "water supply": "Water supply",
    "hotel": "Hotel stay",
    "hotel stay": "Hotel stay",
    "accommodation": "Hotel stay",
    "flight": "Flight",
    "air travel": "Flight",
    "plane": "Flight",
    "rail": "Rail",
    "train": "Rail",
    "taxi": "Taxi / car",
    "car": "Taxi / car",
    "car travel": "Taxi / car",
    "business miles": "Taxi / car",
    "mileage": "Taxi / car",
    "vehicle": "Taxi / car",
    "generic travel spend": "Generic travel spend",
    "travel spend": "Generic travel spend",
    "general waste": "General waste",
    "landfill": "Landfill waste",
    "landfill waste": "Landfill waste",
    "recycling": "Recycling",
    "incineration": "Incineration",
}

UNIT_ALIASES = {
    "kwh": "kWh",
    "kw h": "kWh",
    "kwh ": "kWh",
    " kwh ": "kWh",
    "kwh (gross cv)": "kWh (Gross CV)",
    "gross cv": "kWh (Gross CV)",
    "gross": "kWh (Gross CV)",
    "kwh gross": "kWh (Gross CV)",
    "kwh gross cv": "kWh (Gross CV)",
    "kwh (net cv)": "kWh (Net CV)",
    "net cv": "kWh (Net CV)",
    "net": "kWh (Net CV)",
    "litre": "litres",
    "litres": "litres",
    "liter": "litres",
    "liters": "litres",
    "l": "litres",
    "cubic metre": "cubic metres",
    "cubic metres": "cubic metres",
    "m3": "cubic metres",
    "m³": "cubic metres",
    "million litres": "million litres",
    "tonne": "tonnes",
    "tonnes": "tonnes",
    "t": "tonnes",
    "kg": "kg",
    "kilogram": "kg",
    "kilograms": "kg",
    "room night": "room_nights",
    "room nights": "room_nights",
    "night": "room_nights",
    "nights": "room_nights",
    "gbp": "gbp",
    "£": "gbp",
    "spend": "gbp",
    "pound": "gbp",
    "pounds": "gbp",
    "km": "vehicle_km",
    "kilometre": "vehicle_km",
    "kilometres": "vehicle_km",
    "vehicle km": "vehicle_km",
    "vehicle_km": "vehicle_km",
    "mile": "miles",
    "miles": "miles",
    "business miles": "miles",
    "passenger km": "passenger_km",
    "passenger_km": "passenger_km",
    "passenger kilometre": "passenger_km",
    "passenger kilometres": "passenger_km",
}


# ============================================================
# BASIC HELPERS
# ============================================================
def safe_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none", "null"}:
        return None
    return text


def is_blank(value: Any) -> bool:
    return safe_text(value) is None


def validate_text_input(value: Any, field_name: str) -> str:
    text = safe_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required.")
    return text


def validate_amount(amount: Any) -> float:
    if amount is None:
        raise ValueError("amount is required.")
    try:
        if isinstance(amount, str):
            amount = amount.replace(",", "").replace("£", "").strip()
        value = float(amount)
    except Exception:
        raise ValueError("amount must be numeric.")
    if value < 0:
        raise ValueError("amount cannot be negative.")
    return value


def validate_year(year: Any) -> int:
    if year is None or safe_text(year) is None:
        return DEFAULT_FACTOR_YEAR
    try:
        value = int(float(str(year).strip()))
    except Exception:
        raise ValueError("year must be an integer.")
    if value < 1900 or value > 2100:
        raise ValueError("year is out of valid range.")
    return value


def normalise_key(value: Any) -> str:
    text = safe_text(value)
    return text.lower().strip() if text else ""


def normalize_category(value: Any) -> Optional[str]:
    key = normalise_key(value)
    return CATEGORY_ALIASES.get(key)


def normalize_item(value: Any) -> Optional[str]:
    key = normalise_key(value)
    if not key:
        return None
    return ITEM_ALIASES.get(key, safe_text(value))


def normalize_unit(value: Any) -> Optional[str]:
    key = normalise_key(value)
    if not key:
        return None
    return UNIT_ALIASES.get(key, safe_text(value))


def normalize_amount_and_unit(amount: float, unit: str, category: Optional[str] = None) -> Tuple[float, str, Optional[str]]:
    """Normalise units and convert where needed.

    Returns: amount, unit, conversion_note
    """
    normalized_unit = normalize_unit(unit)
    if normalized_unit is None:
        raise ValueError("unit is required.")

    if normalized_unit == "miles":
        converted = amount * 1.60934
        target_unit = "vehicle_km" if category in {None, "business travel"} else "vehicle_km"
        return converted, target_unit, f"Converted miles to vehicle_km using 1 mile = 1.60934 km. Original amount: {amount} miles."

    return amount, normalized_unit, None


def get_factor_year(activity: Dict[str, Any]) -> int:
    # Accept several field names so transformed client sheets work naturally.
    for key in ["factor_year", "year", "reporting_year"]:
        if safe_text(activity.get(key)) is not None:
            return validate_year(activity.get(key))
    return DEFAULT_FACTOR_YEAR


def row_hash(data: Dict[str, Any]) -> str:
    material = "|".join(f"{k}={data.get(k)}" for k in sorted(data.keys()))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


# ============================================================
# CATEGORY / ITEM INFERENCE
# ============================================================
def infer_canonical_category(activity: Dict[str, Any]) -> Dict[str, Any]:
    raw_category = safe_text(activity.get("category"))
    candidates = [
        activity.get("category"),
        activity.get("item_name"),
        activity.get("sub_category"),
        activity.get("source_section"),
        activity.get("notes"),
    ]

    for value in candidates:
        mapped = normalize_category(value)
        if mapped:
            return {
                "raw_category": raw_category,
                "canonical_category": mapped,
                "mapping_source": "category" if safe_text(value) == raw_category else "inferred",
                "mapping_confidence": "high" if safe_text(value) == raw_category else "medium",
            }

    return {
        "raw_category": raw_category,
        "canonical_category": None,
        "mapping_source": "unmapped",
        "mapping_confidence": "low",
    }


def infer_item_name(activity: Dict[str, Any], canonical_category: str) -> str:
    candidates = [
        activity.get("item_name"),
        activity.get("fuel"),
        activity.get("travel_type"),
        activity.get("waste_type"),
        activity.get("water_type"),
        activity.get("sub_category"),
        activity.get("category"),
        activity.get("source_section"),
        activity.get("notes"),
    ]

    for value in candidates:
        item = normalize_item(value)
        if item:
            # Avoid returning generic category labels where a better default exists.
            if item.lower() in {"fuel", "business travel", "travel", "waste", "water"}:
                continue
            return item

    defaults = {
        "fuel": "Natural gas",
        "electricity": "Electricity: UK",
        "water": "Water supply",
        "business travel": "Generic travel spend",
        "waste": "General waste",
    }
    return defaults.get(canonical_category, "Unknown")


# ============================================================
# FACTOR MATCHING
# ============================================================
def _candidate_years(year: int) -> List[int]:
    # Prefer requested year. Fall back to default year, then any available year.
    years = [year]
    if DEFAULT_FACTOR_YEAR not in years:
        years.append(DEFAULT_FACTOR_YEAR)
    for y in sorted(SUPPORTED_FACTOR_YEARS, reverse=True):
        if y not in years:
            years.append(y)
    return years


def match_factor(
    category: str,
    item_name: str,
    unit: str,
    factor_year: int,
    factor_table: pd.DataFrame = FACTOR_TABLE,
) -> Tuple[pd.Series, str, str]:
    """Match factor with exact-first, controlled fallback.

    Returns factor row, match_type, match_note.
    """
    category_key = category.strip().lower()
    item_key = item_name.strip().lower()
    unit_key = unit.strip().lower()

    for year in _candidate_years(factor_year):
        subset = factor_table[
            (factor_table["factor_year"].astype(int) == int(year))
            & (factor_table["category"].str.lower() == category_key)
            & (factor_table["item_name"].str.lower() == item_key)
            & (factor_table["unit"].str.lower() == unit_key)
        ]
        if not subset.empty:
            match_type = "exact" if year == factor_year else "year_fallback"
            note = "Exact factor-year match." if year == factor_year else f"Requested {factor_year}, used available {year}."
            return subset.iloc[0], match_type, note

    # Controlled generic fallbacks within category/unit.
    for year in _candidate_years(factor_year):
        subset = factor_table[
            (factor_table["factor_year"].astype(int) == int(year))
            & (factor_table["category"].str.lower() == category_key)
            & (factor_table["unit"].str.lower() == unit_key)
        ]
        if not subset.empty:
            return subset.iloc[0], "generic_category_unit_fallback", (
                f"No exact item match for '{item_name}'. Used generic category/unit factor "
                f"'{subset.iloc[0]['item_name']}' for {year}."
            )

    raise ValueError(
        f"No matching factor found for category='{category}', item_name='{item_name}', "
        f"unit='{unit}', factor_year={factor_year}."
    )


# ============================================================
# CALCULATION
# ============================================================
def classify_calculation_basis(mapping_source: Optional[str], factor_quality: str, match_type: str) -> str:
    if factor_quality in {"official", "official_pending_traceability"} and mapping_source == "category" and match_type == "exact":
        return "exact"
    if factor_quality == "disclosure_only_placeholder":
        return "disclosure_only"
    return "estimated"


def build_result_row(
    activity: Dict[str, Any],
    category: str,
    item_name: str,
    input_amount: float,
    input_unit: str,
    normalized_amount: float,
    normalized_unit: str,
    factor_row: pd.Series,
    factor_year_requested: int,
    emissions_kg: float,
    mapping: Dict[str, Any],
    conversion_note: Optional[str],
    match_type: str,
    match_note: str,
) -> pd.DataFrame:
    factor = float(factor_row["kgCO2e_per_unit"])
    factor_quality = str(factor_row.get("factor_quality", "unknown"))
    calculation_basis = classify_calculation_basis(mapping.get("mapping_source"), factor_quality, match_type)

    formula = f"{normalized_amount} {normalized_unit} × {factor} kgCO2e/{normalized_unit} = {emissions_kg} kgCO2e"

    output = {
        "category": factor_row.get("display_category", category.title()),
        "canonical_category": category,
        "sub_category": factor_row.get("activity_group"),
        "item_name": factor_row.get("item_name", item_name),
        "scope": factor_row.get("scope"),
        "input_amount": input_amount,
        "input_unit": input_unit,
        "normalized_amount": normalized_amount,
        "normalized_unit": normalized_unit,
        "unit_conversion_note": conversion_note,
        "emission_factor_kgCO2e_per_unit": factor,
        "factor_year_requested": factor_year_requested,
        "factor_year": int(factor_row.get("factor_year")),
        "emissions_kgCO2e": emissions_kg,
        "emissions_tCO2e": emissions_kg / 1000,
        "calculation_formula": formula,
        "factor_id": factor_row.get("factor_id"),
        "factor_name": f"{factor_row.get('item_name')} / {factor_row.get('unit')}",
        "factor_description": factor_row.get("factor_description", ""),
        "factor_source": factor_row.get("source_name"),
        "data_source": factor_row.get("source_name"),
        "factor_source_file": factor_row.get("source_file"),
        "factor_source_sheet": factor_row.get("source_sheet"),
        "factor_source_row": factor_row.get("source_row"),
        "factor_quality": factor_quality,
        "factor_match_type": match_type,
        "factor_match_note": match_note,
        "original_category": mapping.get("raw_category"),
        "mapping_source": mapping.get("mapping_source"),
        "mapping_confidence": mapping.get("mapping_confidence"),
        "calculation_basis": calculation_basis,
        "engine_version": ENGINE_VERSION,
        "calculated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_row_hash": row_hash(activity),
    }

    for field in METADATA_FIELDS:
        output[field] = activity.get(field, None)

    return pd.DataFrame([output])


def calculate_emissions(activity: Dict[str, Any]) -> pd.DataFrame:
    mapping = infer_canonical_category(activity)
    canonical_category = mapping["canonical_category"]

    if not canonical_category:
        raise ValueError(
            f"Unsupported or unmapped category: '{mapping.get('raw_category')}'. {SUPPORTED_CATEGORY_HINT}"
        )

    input_unit = validate_text_input(activity.get("unit"), "unit")
    input_amount = validate_amount(activity.get("amount"))
    factor_year = get_factor_year(activity)
    item_name = infer_item_name(activity, canonical_category)

    normalized_amount, normalized_unit, conversion_note = normalize_amount_and_unit(
        input_amount, input_unit, category=canonical_category
    )

    # Natural gas kWh defaults to gross CV unless transformer/user specifies net/gross.
    if canonical_category == "fuel" and item_name == "Natural gas" and normalized_unit == "kWh":
        normalized_unit = "kWh (Gross CV)"
        conversion_note = (conversion_note + " " if conversion_note else "") + "Natural gas kWh defaulted to Gross CV."

    # EV charging is electricity; keep item as Electricity: UK unless a specific EV factor exists.
    if canonical_category == "electricity" and item_name in {"EV charging", "EV charger"}:
        item_name = "Electricity: UK"

    factor_row, match_type, match_note = match_factor(
        category=canonical_category,
        item_name=item_name,
        unit=normalized_unit,
        factor_year=factor_year,
    )

    factor = float(factor_row["kgCO2e_per_unit"])
    emissions_kg = normalized_amount * factor

    return build_result_row(
        activity=activity,
        category=canonical_category,
        item_name=item_name,
        input_amount=input_amount,
        input_unit=input_unit,
        normalized_amount=normalized_amount,
        normalized_unit=normalized_unit,
        factor_row=factor_row,
        factor_year_requested=factor_year,
        emissions_kg=emissions_kg,
        mapping=mapping,
        conversion_note=conversion_note,
        match_type=match_type,
        match_note=match_note,
    )


# ============================================================
# PREPROCESSING
# ============================================================
def preprocess_uploaded_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()

    # Normalise column names.
    df_clean.columns = [
        str(col).strip().lower().replace("\n", " ").replace("\r", " ")
        for col in df_clean.columns
    ]

    object_cols = df_clean.select_dtypes(include=["object"]).columns
    for col in object_cols:
        df_clean[col] = df_clean[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    df_clean = df_clean.replace(r"^\s*$", None, regex=True)

    if "category" in df_clean.columns:
        df_clean["category"] = df_clean["category"].apply(lambda x: normalise_key(x) if isinstance(x, str) else x)

    if "unit" in df_clean.columns:
        df_clean["unit"] = df_clean["unit"].apply(lambda x: normalize_unit(x) if isinstance(x, str) else x)

    if "item_name" in df_clean.columns:
        df_clean["item_name"] = df_clean["item_name"].apply(lambda x: normalize_item(x) if isinstance(x, str) else x)

    df_clean = df_clean.where(pd.notnull(df_clean), None)
    return df_clean


# ============================================================
# ERROR GROUPING / QUALITY
# ============================================================
def infer_issue_type_from_message(message: Any) -> str:
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
    if "no matching factor found" in msg:
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
    medium_types = {"Invalid year", "Negative amount", "Other validation issue"}
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
    return {
        "Unmapped category": "Standardise category labels or extend the alias mapping table.",
        "Missing amount": "Add a valid numeric amount for the affected rows before re-uploading.",
        "Non-numeric amount": "Replace text or malformed values in amount with clean numeric values.",
        "Negative amount": "Check whether the input quantity should be positive; handle credits separately.",
        "Missing unit": "Populate the missing unit and ensure it matches the supported unit list.",
        "Factor matching failure": "Review category, item, unit, and factor year so the correct emission factor can be matched.",
        "Invalid year": "Use a valid integer year. Supported configured years are 2023, 2024, and 2025.",
        "Missing required field": "Complete the missing required columns or fields before submitting the dataset.",
        "Other validation issue": "Review affected rows and correct source data before re-uploading.",
    }.get(issue_type, "Review affected rows and correct source data before re-uploading.")


def group_errors(errors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not errors:
        return []
    grouped: Dict[str, Dict[str, Any]] = {}
    for err in errors:
        issue_type = infer_issue_type_from_message(err.get("error"))
        item = grouped.setdefault(
            issue_type,
            {
                "title": issue_type,
                "count": 0,
                "severity": "low",
                "guidance": infer_issue_guidance(issue_type),
                "examples": [],
            },
        )
        item["count"] += 1
        example_bits = []
        if err.get("source_file"):
            example_bits.append(f"[{err['source_file']}]")
        if err.get("row_index_original") is not None:
            example_bits.append(f"row {err['row_index_original']}")
        elif err.get("row_index") is not None:
            example_bits.append(f"row {err['row_index']}")
        if err.get("error"):
            example_bits.append(str(err["error"]))
        example_text = " - ".join(example_bits).strip()
        if example_text and len(item["examples"]) < 5:
            item["examples"].append(example_text)

    for issue_type, item in grouped.items():
        item["severity"] = infer_severity(issue_type, item["count"])

    severity_order = {"high": 0, "medium": 1, "low": 2}
    return sorted(grouped.values(), key=lambda x: (severity_order.get(x["severity"], 9), -x["count"], x["title"]))


def build_error_summary(errors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{"error": item["title"], "count": item["count"]} for item in group_errors(errors)]


def build_errors_preview(errors: List[Dict[str, Any]], max_items: int = 10) -> List[Dict[str, Any]]:
    return errors[:max_items]


# ============================================================
# BATCH PROCESSING
# ============================================================
def calculate_confidence_score(report_df: pd.DataFrame, data_quality: Dict[str, Any]) -> Dict[str, Any]:
    total_rows = int(data_quality.get("total_rows", 0) or 0)
    coverage_percent = float(data_quality.get("coverage_percent", 0.0) or 0.0)
    inferred_rows = int(data_quality.get("inferred_category_rows", 0) or 0)
    estimated_rows = int(data_quality.get("estimated_rows", 0) or 0)
    placeholder_rows = int(data_quality.get("starter_factor_rows", 0) or 0)
    fallback_rows = int(data_quality.get("factor_fallback_rows", 0) or 0)

    if total_rows <= 0:
        return {"score": 0, "label": "Low", "coverage_percent": 0.0, "notes": ["No valid rows were available for calculation."]}

    score = 100.0
    score -= max(0.0, 100.0 - coverage_percent) * 0.45
    score -= (inferred_rows / total_rows) * 15.0
    score -= (estimated_rows / total_rows) * 20.0
    score -= (placeholder_rows / total_rows) * 25.0
    score -= (fallback_rows / total_rows) * 10.0
    score = round(max(0.0, min(100.0, score)), 0)

    label = "High" if score >= 80 else "Moderate" if score >= 60 else "Low"
    notes: List[str] = []
    if coverage_percent >= 95:
        notes.append("Most rows were processed successfully.")
    elif coverage_percent > 0:
        notes.append("Some rows were excluded due to validation or factor-matching issues.")
    if inferred_rows > 0:
        notes.append(f"{inferred_rows} row(s) relied on inferred category mapping.")
    if estimated_rows > 0:
        notes.append(f"{estimated_rows} row(s) used estimated or non-exact treatment.")
    if placeholder_rows > 0:
        notes.append(f"{placeholder_rows} row(s) used starter/placeholder factor rows; replace these with official factor-library rows.")
    if fallback_rows > 0:
        notes.append(f"{fallback_rows} row(s) used factor fallback matching.")
    if not notes:
        notes.append("No material data quality limitations were detected in the returned result set.")

    return {"score": int(score), "label": label, "coverage_percent": coverage_percent, "notes": notes}


def calculate_emissions_batch_safe(activity_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    results: List[pd.DataFrame] = []
    errors: List[Dict[str, Any]] = []
    category_mapping_rows: List[Dict[str, Any]] = []
    unmapped_categories: List[Any] = []

    for i, activity in enumerate(activity_list):
        try:
            activity_data = dict(activity)
            mapping = infer_canonical_category(activity_data)
            category_mapping_rows.append({
                "row_index": i,
                "original_category": mapping.get("raw_category"),
                "canonical_category": mapping.get("canonical_category"),
                "mapping_source": mapping.get("mapping_source"),
                "mapping_confidence": mapping.get("mapping_confidence"),
            })
            if not mapping.get("canonical_category"):
                unmapped_categories.append(mapping.get("raw_category"))
                raise ValueError(f"Unsupported or unmapped category: '{mapping.get('raw_category')}'. {SUPPORTED_CATEGORY_HINT}")

            result = calculate_emissions(activity_data).copy()
            result["row_index"] = i
            results.append(result)

        except Exception as e:
            errors.append({
                "row_index": i,
                "row_index_original": activity.get("row_index_original", i + 1),
                "source_file": activity.get("source_file"),
                "source_section": activity.get("source_section"),
                "original_category": activity.get("category"),
                "input": activity,
                "error": str(e),
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
    mapped_rows = int(mapping_df["canonical_category"].notna().sum()) if not mapping_df.empty else 0
    inferred_rows = int((mapping_df["mapping_source"] == "inferred").sum()) if not mapping_df.empty else 0

    unmapped_summary: List[Dict[str, Any]] = []
    if unmapped_categories:
        unmapped_df = pd.Series(unmapped_categories, dtype="object").fillna("blank").value_counts().reset_index()
        unmapped_df.columns = ["category", "count"]
        unmapped_summary = unmapped_df.to_dict(orient="records")

    exact_factor_rows = estimated_rows = starter_factor_rows = official_factor_rows = fallback_rows = 0
    disclosure_rows = 0

    if not final_report.empty:
        exact_factor_rows = int((final_report.get("calculation_basis") == "exact").sum())
        estimated_rows = int((final_report.get("calculation_basis") == "estimated").sum())
        disclosure_rows = int((final_report.get("calculation_basis") == "disclosure_only").sum())
        starter_factor_rows = int(final_report["factor_quality"].astype(str).str.contains("placeholder", case=False, na=False).sum())
        official_factor_rows = int(final_report["factor_quality"].astype(str).str.contains("official", case=False, na=False).sum())
        fallback_rows = int((final_report["factor_match_type"] != "exact").sum()) if "factor_match_type" in final_report.columns else 0

    data_quality = {
        "total_rows": total_rows,
        "successful_rows": successful_rows,
        "errored_rows": errored_rows,
        "coverage_percent": coverage_percent,
        "mapped_category_rows": mapped_rows,
        "inferred_category_rows": inferred_rows,
        "exact_factor_rows": exact_factor_rows,
        "estimated_rows": estimated_rows,
        "disclosure_only_rows": disclosure_rows,
        "official_factor_rows": official_factor_rows,
        "starter_factor_rows": starter_factor_rows,
        "factor_fallback_rows": fallback_rows,
        "valid_rows_percent": coverage_percent,
        "invalid_rows_percent": round((errored_rows / total_rows) * 100, 2) if total_rows > 0 else 0.0,
    }

    confidence = calculate_confidence_score(final_report, data_quality)

    return {
        "report": final_report,
        "total_kgCO2e": total_kg,
        "total_tCO2e": total_t,
        "errors": errors,
        "data_quality": data_quality,
        "unmapped_categories": unmapped_summary,
        "confidence": confidence,
    }


# ============================================================
# SUMMARIES / RESPONSE BUILDERS
# ============================================================
def summarize_report_by_scope_and_category(report_df: pd.DataFrame) -> pd.DataFrame:
    if report_df.empty:
        return pd.DataFrame(columns=["scope", "category", "emissions_kgCO2e", "emissions_tCO2e"])
    return (
        report_df.groupby(["scope", "category"], as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
        .sum()
        .sort_values(["scope", "category"])
        .reset_index(drop=True)
    )


def summarize_report_by_scope(report_df: pd.DataFrame) -> pd.DataFrame:
    if report_df.empty:
        return pd.DataFrame(columns=["scope", "emissions_kgCO2e", "emissions_tCO2e", "percent_of_total"])
    summary = (
        report_df.groupby("scope", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
        .sum()
        .sort_values("emissions_kgCO2e", ascending=False)
        .reset_index(drop=True)
    )
    total_kg = float(summary["emissions_kgCO2e"].sum()) if not summary.empty else 0.0
    summary["percent_of_total"] = summary["emissions_kgCO2e"].apply(lambda x: round((float(x) / total_kg) * 100, 2) if total_kg > 0 else 0.0)
    return summary


def summarize_top_categories(report_df: pd.DataFrame, top_n: int = 5) -> List[Dict[str, Any]]:
    if report_df.empty:
        return []
    df = (
        report_df.groupby("category", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
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
        working.groupby("site_name", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
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
        "category", "canonical_category", "scope", "factor_id", "factor_name", "factor_description",
        "emission_factor_kgCO2e_per_unit", "normalized_unit", "factor_year", "factor_year_requested",
        "factor_source", "factor_source_file", "factor_source_sheet", "factor_source_row",
        "factor_quality", "factor_match_type", "factor_match_note", "calculation_basis",
    ]
    available_cols = [c for c in cols if c in report_df.columns]
    factor_df = report_df[available_cols].drop_duplicates().reset_index(drop=True)
    sort_cols = [c for c in ["category", "factor_name", "factor_year"] if c in factor_df.columns]
    if sort_cols:
        factor_df = factor_df.sort_values(by=sort_cols).reset_index(drop=True)
    return factor_df.to_dict(orient="records")


def build_analytics(report_df: pd.DataFrame) -> Dict[str, Any]:
    analytics: Dict[str, Any] = {}
    if report_df.empty:
        analytics["average_emissions_per_row_kgco2e"] = 0.0
        return analytics
    analytics["average_emissions_per_row_kgco2e"] = round(float(report_df["emissions_kgCO2e"].mean()), 4)

    category_df = (
        report_df.groupby("category", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
        .sum().sort_values("emissions_kgCO2e", ascending=False).reset_index(drop=True)
    )
    if not category_df.empty:
        analytics["top_category_by_kgco2e"] = category_df.iloc[0].to_dict()
        analytics["top_categories_by_kgco2e"] = category_df.head(5).to_dict(orient="records")

    if "site_name" in report_df.columns:
        site_df = (
            report_df.assign(site_name=report_df["site_name"].fillna("Unspecified site"))
            .groupby("site_name", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
            .sum().sort_values("emissions_kgCO2e", ascending=False).reset_index(drop=True)
        )
        if not site_df.empty:
            analytics["top_site_by_kgco2e"] = site_df.iloc[0].to_dict()
            analytics["top_sites_by_kgco2e"] = site_df.head(5).to_dict(orient="records")

    if "scope" in report_df.columns:
        scope_df = (
            report_df.groupby("scope", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
            .sum().sort_values("emissions_kgCO2e", ascending=False).reset_index(drop=True)
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
    if total_kg > 0:
        takeaways.append(f"Total reported emissions are {total_t} tCO2e ({total_kg} kgCO2e) across the processed dataset.")
    if not summary_scope.empty:
        top_scope = summary_scope.iloc[0]
        takeaways.append(f"{top_scope['scope']} is the largest contributor, representing {round(float(top_scope['percent_of_total']), 2)}% of total emissions.")
    top_category = analytics.get("top_category_by_kgco2e")
    if top_category:
        takeaways.append(f"The highest-emitting category is {top_category.get('category')}, contributing {round(float(top_category.get('emissions_kgCO2e', 0.0)), 2)} kgCO2e.")
    top_site = analytics.get("top_site_by_kgco2e")
    if top_site:
        takeaways.append(f"The highest-emitting site is {top_site.get('site_name')}, contributing {round(float(top_site.get('emissions_kgCO2e', 0.0)), 2)} kgCO2e.")
    coverage = data_quality.get("coverage_percent", 0.0)
    if coverage > 0:
        takeaways.append(f"Data processing coverage is {coverage}%, indicating the share of uploaded rows included in the calculation.")
    if confidence:
        takeaways.append(f"The current confidence score is {confidence.get('score', 0)}/100, rated {confidence.get('label', 'Low')}.")
    return takeaways[:6]


def build_actionable_insights(report_df: pd.DataFrame, batch_result: Dict[str, Any], summary_scope: pd.DataFrame, analytics: Dict[str, Any]) -> List[Dict[str, Any]]:
    insights: List[Dict[str, Any]] = []
    data_quality = batch_result.get("data_quality", {})
    confidence = batch_result.get("confidence", {})
    unmapped_categories = batch_result.get("unmapped_categories", [])
    if not summary_scope.empty:
        top_scope = summary_scope.iloc[0]
        insights.append({
            "title": "Largest emission driver",
            "body": f"{top_scope['scope']} is currently the biggest contributor at {round(float(top_scope['emissions_kgCO2e']), 2)} kgCO2e ({round(float(top_scope['percent_of_total']), 2)}% of total emissions).",
        })
    top_category = analytics.get("top_category_by_kgco2e")
    if top_category:
        insights.append({"title": "Priority reduction opportunity", "body": f"The category with the greatest impact is {top_category.get('category')}. Targeting this area is likely to create the largest near-term reduction effect."})
    top_site = analytics.get("top_site_by_kgco2e")
    if top_site:
        insights.append({"title": "Operational hotspot", "body": f"{top_site.get('site_name')} appears to be the highest-emitting site. A site-specific review could help identify concentrated savings opportunities."})
    if data_quality.get("coverage_percent", 0.0) < 90 and data_quality.get("total_rows", 0) > 0:
        insights.append({"title": "Data completeness risk", "body": f"Coverage is currently {data_quality.get('coverage_percent', 0.0)}%. Some records were excluded, so totals may be incomplete until input issues are corrected."})
    if data_quality.get("starter_factor_rows", 0) > 0:
        insights.append({"title": "Placeholder factor library in use", "body": f"{data_quality.get('starter_factor_rows', 0)} row(s) used starter or placeholder factors. Replace them with official DEFRA flat-file factors before audit/reporting use."})
    if data_quality.get("factor_fallback_rows", 0) > 0:
        insights.append({"title": "Factor fallback matching present", "body": f"{data_quality.get('factor_fallback_rows', 0)} row(s) used fallback factor matching. Review these line items before external reporting."})
    if unmapped_categories:
        top_unmapped = unmapped_categories[0]
        insights.append({"title": "Category mapping improvement needed", "body": f"Some categories remain unmapped. For example, '{top_unmapped.get('category')}' appears {top_unmapped.get('count', 0)} time(s)."})
    if confidence and confidence.get("score", 0) < 60:
        insights.append({"title": "Low-confidence result", "body": f"The current confidence score is {confidence.get('score', 0)}/100. Input cleanup and official factor replacement should be prioritised."})
    return insights[:8]


def build_methodology_summary(batch_result: Dict[str, Any]) -> Dict[str, Any]:
    data_quality = batch_result.get("data_quality", {})
    return {
        "framework": "GHG Protocol aligned reporting structure using uploaded activity data and emissions factors.",
        "factor_basis": "The engine is structured for DEFRA/UK Government 2023, 2024 and 2025 factor years. Built-in starter rows should be replaced by official flat-file factor rows for audit use.",
        "calculation_logic": "Each valid input row is mapped to a canonical emissions category, normalised to a supported unit, matched to an emissions factor, multiplied by the activity quantity, and aggregated into CO2e outputs.",
        "assumptions": [
            "Natural gas kWh defaults to Gross CV unless otherwise specified.",
            "Miles are converted to vehicle kilometres using 1 mile = 1.60934 km.",
        ],
        "interpretation_notes": [
            "Results should be reviewed alongside confidence, data quality, factor transparency, and errors/exclusions.",
            "Placeholder or fallback factor matches should not be used for final audit reporting until replaced or approved.",
        ],
        "engine_version": ENGINE_VERSION,
        "data_quality_snapshot": data_quality,
    }


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
        "totals": {"total_kgCO2e": batch_result["total_kgCO2e"], "total_tCO2e": batch_result["total_tCO2e"]},
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
        "methodology_summary": methodology_summary,
        "engine_version": ENGINE_VERSION,
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
    if pd.isna(obj) if isinstance(obj, float) else False:
        return None
    return obj


def run_emissions_engine(request_json: Dict[str, Any]) -> Dict[str, Any]:
    activities = request_json.get("activities", [])
    if not isinstance(activities, list):
        raise ValueError("activities must be a list")
    batch_result = calculate_emissions_batch_safe(activities)
    final_response = build_final_response(batch_result)
    return make_json_safe(final_response)


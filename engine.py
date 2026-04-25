"""
engine.py - Production-oriented carbon emissions calculation engine

Version: 3.0.0

Designed for:
- Canonical activity-row inputs: category, amount, unit, item_name/fuel/country/etc.
- Semi-structured workbook-like records that have been flattened into row dictionaries.
- DEFRA / UK Government factor-year handling for 2023, 2024, 2025.
- Strong audit trail: calculation formula, input hash, factor id/source/year/match type.
- Report-ready API output: line items, totals, summaries, insights, data quality, errors.
- Conservative double-counting controls for fleet fuel, business mileage, and EV charging.

Important production note:
This engine includes starter built-in factor rows to keep your Render app working.
For true external audit/reporting use, replace or supplement BUILTIN_FACTOR_ROWS with
official UK Government conversion-factor flat-file rows using a factor_loader.py module.
The engine is deliberately structured so that replacement is straightforward.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


# ============================================================
# VERSIONING / GLOBALS
# ============================================================
ENGINE_VERSION = "3.0.0"
DEFAULT_FACTOR_YEAR = 2025
SUPPORTED_FACTOR_YEARS = {2023, 2024, 2025}
DEFAULT_COUNTRY = "Electricity: UK"

SUPPORTED_CATEGORY_HINT = (
    "Supported / mappable categories include: fuel, liquid fuel, electricity, "
    "water, business travel, travel, hotel, waste, EV charging, fleet fuel."
)

METADATA_FIELDS = [
    "record_id",
    "client_name",
    "company_name",
    "site_name",
    "reporting_period",
    "reporting_year",
    "period_start",
    "period_end",
    "notes",
    "source_file",
    "source_sheet",
    "source_workbook",
    "source_section",
    "row_index_original",
    "raw_column",
    "raw_row_label",
    "invoice_reference",
    "account_reference",
    "supplier",
    "meter_id",
    "raw_vehicle",
    "vehicle_type_hint",
    "vehicle_class",
    "vehicle_registration",
    "driver_name",
    "cost_centre",
    "department",
]


# ============================================================
# BUILT-IN FACTOR LIBRARY
# ============================================================
# Values preserve your known factors and add a few starter operational factors.
# Every row is explicit about quality. Do not call starter rows audit-final.

BUILTIN_FACTOR_ROWS: List[Dict[str, Any]] = [
    # Natural gas
    {
        "factor_id": "uk-2025-fuel-natural-gas-gross-kwh",
        "factor_year": 2025,
        "category": "fuel",
        "display_category": "Fuel",
        "scope": "Scope 1",
        "activity_group": "Gaseous fuels",
        "item_name": "Natural gas",
        "unit": "kWh (Gross CV)",
        "kgCO2e_per_unit": 0.18296,
        "factor_quality": "official_pending_traceability",
        "source_name": "UK Government GHG conversion factors 2025 - built-in row pending official flat-file trace",
        "source_file": "built_in_engine_table",
        "source_sheet": "fuel",
        "source_row": None,
        "factor_description": "Natural gas combustion factor, gross CV.",
    },
    {
        "factor_id": "uk-2025-fuel-natural-gas-net-kwh",
        "factor_year": 2025,
        "category": "fuel",
        "display_category": "Fuel",
        "scope": "Scope 1",
        "activity_group": "Gaseous fuels",
        "item_name": "Natural gas",
        "unit": "kWh (Net CV)",
        "kgCO2e_per_unit": 0.20270,
        "factor_quality": "official_pending_traceability",
        "source_name": "UK Government GHG conversion factors 2025 - built-in row pending official flat-file trace",
        "source_file": "built_in_engine_table",
        "source_sheet": "fuel",
        "source_row": None,
        "factor_description": "Natural gas combustion factor, net CV.",
    },
    {
        "factor_id": "uk-2025-fuel-natural-gas-m3",
        "factor_year": 2025,
        "category": "fuel",
        "display_category": "Fuel",
        "scope": "Scope 1",
        "activity_group": "Gaseous fuels",
        "item_name": "Natural gas",
        "unit": "cubic metres",
        "kgCO2e_per_unit": 2.06672,
        "factor_quality": "official_pending_traceability",
        "source_name": "UK Government GHG conversion factors 2025 - built-in row pending official flat-file trace",
        "source_file": "built_in_engine_table",
        "source_sheet": "fuel",
        "source_row": None,
        "factor_description": "Natural gas combustion factor, cubic metres.",
    },
    {
        "factor_id": "uk-2025-fuel-natural-gas-tonnes",
        "factor_year": 2025,
        "category": "fuel",
        "display_category": "Fuel",
        "scope": "Scope 1",
        "activity_group": "Gaseous fuels",
        "item_name": "Natural gas",
        "unit": "tonnes",
        "kgCO2e_per_unit": 2575.46441,
        "factor_quality": "official_pending_traceability",
        "source_name": "UK Government GHG conversion factors 2025 - built-in row pending official flat-file trace",
        "source_file": "built_in_engine_table",
        "source_sheet": "fuel",
        "source_row": None,
        "factor_description": "Natural gas combustion factor, tonnes.",
    },

    # Electricity. 2023/2024 are placeholders until official rows are loaded.
    {
        "factor_id": "uk-2023-electricity-uk-kwh-placeholder",
        "factor_year": 2023,
        "category": "electricity",
        "display_category": "Electricity",
        "scope": "Scope 2",
        "activity_group": "Electricity generated",
        "item_name": "Electricity: UK",
        "unit": "kWh",
        "kgCO2e_per_unit": 0.20705,
        "factor_quality": "starter_placeholder",
        "source_name": "Starter value - replace with official UK Government 2023 flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "electricity",
        "source_row": None,
        "factor_description": "UK location-based electricity factor placeholder.",
    },
    {
        "factor_id": "uk-2024-electricity-uk-kwh-placeholder",
        "factor_year": 2024,
        "category": "electricity",
        "display_category": "Electricity",
        "scope": "Scope 2",
        "activity_group": "Electricity generated",
        "item_name": "Electricity: UK",
        "unit": "kWh",
        "kgCO2e_per_unit": 0.20705,
        "factor_quality": "starter_placeholder",
        "source_name": "Starter value - replace with official UK Government 2024 flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "electricity",
        "source_row": None,
        "factor_description": "UK location-based electricity factor placeholder.",
    },
    {
        "factor_id": "uk-2025-electricity-uk-kwh",
        "factor_year": 2025,
        "category": "electricity",
        "display_category": "Electricity",
        "scope": "Scope 2",
        "activity_group": "Electricity generated",
        "item_name": "Electricity: UK",
        "unit": "kWh",
        "kgCO2e_per_unit": 0.177,
        "factor_quality": "official_pending_traceability",
        "source_name": "UK Government GHG conversion factors 2025 - built-in row pending official flat-file trace",
        "source_file": "built_in_engine_table",
        "source_sheet": "electricity",
        "source_row": None,
        "factor_description": "UK location-based electricity factor.",
    },

    # Water
    {
        "factor_id": "uk-2025-water-supply-m3",
        "factor_year": 2025,
        "category": "water",
        "display_category": "Water",
        "scope": "Scope 3",
        "activity_group": "Water supply",
        "item_name": "Water supply",
        "unit": "cubic metres",
        "kgCO2e_per_unit": 0.1913,
        "factor_quality": "official_pending_traceability",
        "source_name": "UK Government GHG conversion factors 2025 - built-in row pending official flat-file trace",
        "source_file": "built_in_engine_table",
        "source_sheet": "water",
        "source_row": None,
        "factor_description": "Water supply factor.",
    },
    {
        "factor_id": "uk-2025-water-supply-million-litres",
        "factor_year": 2025,
        "category": "water",
        "display_category": "Water",
        "scope": "Scope 3",
        "activity_group": "Water supply",
        "item_name": "Water supply",
        "unit": "million litres",
        "kgCO2e_per_unit": 191.30156,
        "factor_quality": "official_pending_traceability",
        "source_name": "UK Government GHG conversion factors 2025 - built-in row pending official flat-file trace",
        "source_file": "built_in_engine_table",
        "source_sheet": "water",
        "source_row": None,
        "factor_description": "Water supply factor.",
    },

    # Liquid fuels - starter operational paths
    {
        "factor_id": "uk-2025-liquid-fuel-diesel-litres-starter",
        "factor_year": 2025,
        "category": "liquid fuel",
        "display_category": "Liquid Fuel",
        "scope": "Scope 1",
        "activity_group": "Liquid fuels",
        "item_name": "Diesel",
        "unit": "litres",
        "kgCO2e_per_unit": 2.687,
        "factor_quality": "starter_placeholder",
        "source_name": "Starter direct fuel factor - replace with official UK Government flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "liquid_fuels",
        "source_row": None,
        "factor_description": "Diesel litres direct combustion starter factor.",
    },
    {
        "factor_id": "uk-2025-liquid-fuel-petrol-litres-starter",
        "factor_year": 2025,
        "category": "liquid fuel",
        "display_category": "Liquid Fuel",
        "scope": "Scope 1",
        "activity_group": "Liquid fuels",
        "item_name": "Petrol",
        "unit": "litres",
        "kgCO2e_per_unit": 2.193,
        "factor_quality": "starter_placeholder",
        "source_name": "Starter direct fuel factor - replace with official UK Government flat-file row",
        "source_file": "built_in_engine_table",
        "source_sheet": "liquid_fuels",
        "source_row": None,
        "factor_description": "Petrol litres direct combustion starter factor.",
    },
    {
        "factor_id": "uk-2025-adblue-litres-disclosure-only",
        "factor_year": 2025,
        "category": "liquid fuel",
        "display_category": "Liquid Fuel",
        "scope": "Scope 3",
        "activity_group": "Vehicle consumables",
        "item_name": "AdBlue",
        "unit": "litres",
        "kgCO2e_per_unit": 0.0,
        "factor_quality": "disclosure_only_placeholder",
        "source_name": "No calculation factor configured - disclosure only until approved",
        "source_file": "built_in_engine_table",
        "source_sheet": "vehicle_consumables",
        "source_row": None,
        "factor_description": "AdBlue recorded for transparency but not emissions-calculated by default.",
    },

    # Business travel and mileage - starter only
    {
        "factor_id": "uk-2025-business-travel-hotel-room-nights-starter",
        "factor_year": 2025,
        "category": "business travel",
        "display_category": "Business Travel",
        "scope": "Scope 3",
        "activity_group": "Business travel",
        "item_name": "Hotel stay",
        "unit": "room_nights",
        "kgCO2e_per_unit": 10.40,
        "factor_quality": "starter_placeholder",
        "source_name": "Starter Scope 3 factor - replace with official factor row",
        "source_file": "built_in_engine_table",
        "source_sheet": "business_travel",
        "source_row": None,
        "factor_description": "Hotel room night starter factor.",
    },
    {
        "factor_id": "uk-2025-business-travel-flight-passenger-km-starter",
        "factor_year": 2025,
        "category": "business travel",
        "display_category": "Business Travel",
        "scope": "Scope 3",
        "activity_group": "Business travel",
        "item_name": "Flight",
        "unit": "passenger_km",
        "kgCO2e_per_unit": 0.146,
        "factor_quality": "starter_placeholder",
        "source_name": "Starter Scope 3 factor - replace with official factor row",
        "source_file": "built_in_engine_table",
        "source_sheet": "business_travel",
        "source_row": None,
        "factor_description": "Flight passenger-km starter factor.",
    },
    {
        "factor_id": "uk-2025-business-travel-rail-passenger-km-starter",
        "factor_year": 2025,
        "category": "business travel",
        "display_category": "Business Travel",
        "scope": "Scope 3",
        "activity_group": "Business travel",
        "item_name": "Rail",
        "unit": "passenger_km",
        "kgCO2e_per_unit": 0.035,
        "factor_quality": "starter_placeholder",
        "source_name": "Starter Scope 3 factor - replace with official factor row",
        "source_file": "built_in_engine_table",
        "source_sheet": "business_travel",
        "source_row": None,
        "factor_description": "Rail passenger-km starter factor.",
    },
    {
        "factor_id": "uk-2025-business-travel-car-vehicle-km-starter",
        "factor_year": 2025,
        "category": "business travel",
        "display_category": "Business Travel",
        "scope": "Scope 3",
        "activity_group": "Business travel",
        "item_name": "Taxi / car",
        "unit": "vehicle_km",
        "kgCO2e_per_unit": 0.170,
        "factor_quality": "starter_placeholder",
        "source_name": "Starter Scope 3 factor - replace with official factor row",
        "source_file": "built_in_engine_table",
        "source_sheet": "business_travel",
        "source_row": None,
        "factor_description": "Generic vehicle-km starter factor.",
    },
    {
        "factor_id": "uk-2025-business-travel-gbp-starter",
        "factor_year": 2025,
        "category": "business travel",
        "display_category": "Business Travel",
        "scope": "Scope 3",
        "activity_group": "Business travel",
        "item_name": "Generic travel spend",
        "unit": "gbp",
        "kgCO2e_per_unit": 0.28,
        "factor_quality": "starter_placeholder",
        "source_name": "Starter spend factor - replace with approved EEIO/spend factor if used",
        "source_file": "built_in_engine_table",
        "source_sheet": "business_travel",
        "source_row": None,
        "factor_description": "Generic business-travel spend starter factor.",
    },

    # Waste - starter only
    {
        "factor_id": "uk-2025-waste-general-tonnes-starter",
        "factor_year": 2025,
        "category": "waste",
        "display_category": "Waste",
        "scope": "Scope 3",
        "activity_group": "Waste generated",
        "item_name": "General waste",
        "unit": "tonnes",
        "kgCO2e_per_unit": 250.0,
        "factor_quality": "starter_placeholder",
        "source_name": "Starter waste factor - replace with official factor row",
        "source_file": "built_in_engine_table",
        "source_sheet": "waste",
        "source_row": None,
        "factor_description": "General waste starter factor.",
    },
    {
        "factor_id": "uk-2025-waste-landfill-tonnes-starter",
        "factor_year": 2025,
        "category": "waste",
        "display_category": "Waste",
        "scope": "Scope 3",
        "activity_group": "Waste generated",
        "item_name": "Landfill waste",
        "unit": "tonnes",
        "kgCO2e_per_unit": 458.0,
        "factor_quality": "starter_placeholder",
        "source_name": "Starter waste factor - replace with official factor row",
        "source_file": "built_in_engine_table",
        "source_sheet": "waste",
        "source_row": None,
        "factor_description": "Landfill waste starter factor.",
    },
    {
        "factor_id": "uk-2025-waste-recycling-tonnes-starter",
        "factor_year": 2025,
        "category": "waste",
        "display_category": "Waste",
        "scope": "Scope 3",
        "activity_group": "Waste generated",
        "item_name": "Recycling",
        "unit": "tonnes",
        "kgCO2e_per_unit": 21.0,
        "factor_quality": "starter_placeholder",
        "source_name": "Starter waste factor - replace with official factor row",
        "source_file": "built_in_engine_table",
        "source_sheet": "waste",
        "source_row": None,
        "factor_description": "Recycling starter factor.",
    },
    {
        "factor_id": "uk-2025-waste-incineration-tonnes-starter",
        "factor_year": 2025,
        "category": "waste",
        "display_category": "Waste",
        "scope": "Scope 3",
        "activity_group": "Waste generated",
        "item_name": "Incineration",
        "unit": "tonnes",
        "kgCO2e_per_unit": 27.0,
        "factor_quality": "starter_placeholder",
        "source_name": "Starter waste factor - replace with official factor row",
        "source_file": "built_in_engine_table",
        "source_sheet": "waste",
        "source_row": None,
        "factor_description": "Incineration starter factor.",
    },
]


def _build_builtin_factor_table() -> pd.DataFrame:
    rows = list(BUILTIN_FACTOR_ROWS)

    # Add kg waste equivalents from tonne rows.
    for row in list(rows):
        if row.get("category") == "waste" and row.get("unit") == "tonnes":
            kg = dict(row)
            kg["factor_id"] = str(kg["factor_id"]).replace("tonnes", "kg")
            kg["unit"] = "kg"
            kg["kgCO2e_per_unit"] = float(kg["kgCO2e_per_unit"]) / 1000.0
            kg["factor_description"] = f"{kg.get('factor_description', '').strip()} Converted to kg basis."
            rows.append(kg)

    df = pd.DataFrame(rows)
    df["factor_year"] = pd.to_numeric(df["factor_year"], errors="coerce").fillna(DEFAULT_FACTOR_YEAR).astype(int)
    df["kgCO2e_per_unit"] = pd.to_numeric(df["kgCO2e_per_unit"], errors="coerce").fillna(0.0)
    return df


FACTOR_TABLE = _build_builtin_factor_table()


# ============================================================
# ALIASES / NORMALISATION
# ============================================================
CATEGORY_ALIASES = {
    "fuel": "fuel",
    "gas": "fuel",
    "natural gas": "fuel",
    "gaseous fuel": "fuel",
    "gas charges": "fuel",
    "boiler gas": "fuel",
    "liquid fuel": "liquid fuel",
    "fleet fuel": "liquid fuel",
    "fuel litres": "liquid fuel",
    "diesel": "liquid fuel",
    "diesel litres": "liquid fuel",
    "petrol": "liquid fuel",
    "gas oil": "liquid fuel",
    "derv": "liquid fuel",
    "adblue": "liquid fuel",
    "ad blue": "liquid fuel",
    "electricity": "electricity",
    "electricity usage": "electricity",
    "power": "electricity",
    "grid electricity": "electricity",
    "ev charging": "electricity",
    "ev charger": "electricity",
    "ev electricity": "electricity",
    "electric used by ev": "electricity",
    "water": "water",
    "water supply": "water",
    "water usage": "water",
    "travel": "business travel",
    "business travel": "business travel",
    "business miles": "business travel",
    "mileage": "business travel",
    "hotel": "business travel",
    "hotel stay": "business travel",
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
    "recycle": "waste",
    "incineration": "waste",
}

ITEM_ALIASES = {
    "natural gas": "Natural gas",
    "gas": "Natural gas",
    "gas charges": "Natural gas",
    "electricity": "Electricity: UK",
    "electricity: uk": "Electricity: UK",
    "uk electricity": "Electricity: UK",
    "grid electricity": "Electricity: UK",
    "ev charging": "Electricity: UK",
    "electric used by ev": "Electricity: UK",
    "diesel": "Diesel",
    "diesel litres": "Diesel",
    "fleet fuel": "Diesel",
    "gas oil": "Diesel",
    "road diesel": "Diesel",
    "derv": "Diesel",
    "petrol": "Petrol",
    "gasoline": "Petrol",
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
    "business miles": "Taxi / car",
    "mileage": "Taxi / car",
    "vehicle": "Taxi / car",
    "generic travel spend": "Generic travel spend",
    "travel spend": "Generic travel spend",
    "general waste": "General waste",
    "landfill": "Landfill waste",
    "landfill waste": "Landfill waste",
    "recycling": "Recycling",
    "recycle": "Recycling",
    "incineration": "Incineration",
}

UNIT_ALIASES = {
    "kwh": "kWh",
    "kw h": "kWh",
    "kwhr": "kWh",
    "kwhs": "kWh",
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
    "ltr": "litres",
    "ltrs": "litres",
    "cubic metre": "cubic metres",
    "cubic metres": "cubic metres",
    "m3": "cubic metres",
    "m³": "cubic metres",
    "cu m": "cubic metres",
    "million litres": "million litres",
    "ml": "million litres",
    "tonne": "tonnes",
    "tonnes": "tonnes",
    "t": "tonnes",
    "kg": "kg",
    "kgs": "kg",
    "kilogram": "kg",
    "kilograms": "kg",
    "room night": "room_nights",
    "room nights": "room_nights",
    "night": "room_nights",
    "nights": "room_nights",
    "gbp": "gbp",
    "£": "gbp",
    "pound": "gbp",
    "pounds": "gbp",
    "spend": "gbp",
    "km": "vehicle_km",
    "kilometre": "vehicle_km",
    "kilometres": "vehicle_km",
    "vehicle km": "vehicle_km",
    "vehicle_km": "vehicle_km",
    "mile": "miles",
    "miles": "miles",
    "mi": "miles",
    "business miles": "miles",
    "passenger km": "passenger_km",
    "passenger_km": "passenger_km",
    "passenger kilometre": "passenger_km",
    "passenger kilometres": "passenger_km",
}

VEHICLE_CLASS_RULES = [
    ("car_ev", [r"\belectric\b", r"\bev\b", r"\bid\.?3\b", r"\bid3\b", r"\beqa\b", r"\btesla\b", r"\bleaf\b"]),
    ("car_plugin_hybrid", [r"plug[\s\-]?in hybrid", r"\bphev\b", r"\b330e\b", r"\bgte\b"]),
    ("car_hybrid", [r"\bhybrid\b"]),
    ("hgv_44t", [r"\b44\s*ton", r"\b44t\b", r"\bactros\b", r"articulated"]),
    ("hgv_rigid_26t", [r"\b26\s*ton", r"\b26t\b", r"\brigid\b"]),
    ("van_lcv", [r"\bvan\b", r"\btransit\b", r"\bsprinter\b"]),
    ("pickup_4x4", [r"\bpickup\b", r"\branger\b", r"\bwildtrak\b", r"\bhilux\b"]),
]


# ============================================================
# SAFE VALUE HELPERS
# ============================================================
def safe_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if isinstance(value, float) and pd.isna(value):
            return None
        if pd.isna(value) and not isinstance(value, str):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none", "null", "nat"}:
        return None
    return text


def is_blank(value: Any) -> bool:
    return safe_text(value) is None


def normalise_key(value: Any) -> str:
    text = safe_text(value)
    if text is None:
        return ""
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text


def strip_money_and_commas(value: Any) -> Any:
    if isinstance(value, str):
        return (
            value.replace(",", "")
            .replace("£", "")
            .replace("$", "")
            .replace("€", "")
            .replace("kgCO2e", "")
            .replace("kg CO2e", "")
            .strip()
        )
    return value


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if is_blank(value):
            return default
        return float(strip_money_and_commas(value))
    except Exception:
        return default


def validate_text_input(value: Any, field_name: str) -> str:
    text = safe_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required.")
    return text


def validate_amount(amount: Any) -> float:
    if is_blank(amount):
        raise ValueError("amount is required.")
    try:
        value = float(strip_money_and_commas(amount))
    except Exception:
        raise ValueError("amount must be numeric.")
    if not math.isfinite(value):
        raise ValueError("amount must be finite.")
    if value < 0:
        raise ValueError("amount cannot be negative.")
    return value


def validate_factor_value(value: Any, field_name: str = "emission factor") -> float:
    try:
        factor = float(strip_money_and_commas(value))
    except Exception:
        raise ValueError(f"{field_name} must be numeric.")
    if not math.isfinite(factor):
        raise ValueError(f"{field_name} must be finite.")
    if factor < 0:
        raise ValueError(f"{field_name} cannot be negative.")
    return factor


def validate_year(year: Any) -> int:
    if safe_text(year) is None:
        return DEFAULT_FACTOR_YEAR
    try:
        value = int(float(str(year).strip()))
    except Exception:
        raise ValueError("year must be an integer.")
    if value < 1900 or value > 2100:
        raise ValueError("year is out of valid range.")
    return value


def looks_like_number(value: Any) -> bool:
    if is_blank(value):
        return False
    try:
        float(strip_money_and_commas(value))
        return True
    except Exception:
        return False


def parse_date(value: Any) -> Optional[pd.Timestamp]:
    if is_blank(value):
        return None
    try:
        parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
        if pd.isna(parsed):
            return None
        return pd.Timestamp(parsed)
    except Exception:
        return None


def looks_like_date(value: Any) -> bool:
    return parse_date(value) is not None


def parse_period_bounds(value: Any) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    text = safe_text(value)
    if not text:
        return None, None
    if looks_like_date(text):
        dt = parse_date(text)
        return dt, dt

    parts = re.split(r"\s*(?:-|to|–|—)\s*", text, flags=re.IGNORECASE)
    if len(parts) == 2:
        start = parse_date(parts[0])
        end = parse_date(parts[1])
        return start, end

    return None, None


def serialise_timestamp(value: Optional[pd.Timestamp]) -> Optional[str]:
    if value is None:
        return None
    return pd.Timestamp(value).date().isoformat()


def first_non_blank(*values: Any) -> Any:
    for value in values:
        if not is_blank(value):
            return value
    return None


def row_hash(data: Dict[str, Any]) -> str:
    try:
        material = json.dumps(data, sort_keys=True, default=str)
    except Exception:
        material = "|".join(f"{k}={data.get(k)}" for k in sorted(data.keys()))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


# ============================================================
# NORMALISATION FUNCTIONS
# ============================================================
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


def get_factor_year(activity: Dict[str, Any]) -> int:
    for key in ["factor_year", "year", "reporting_year"]:
        if safe_text(activity.get(key)) is not None:
            return validate_year(activity.get(key))

    # Try period end/start/reporting period.
    for key in ["period_end", "period_start", "reporting_period"]:
        dt = parse_date(activity.get(key))
        if dt is not None:
            return int(dt.year)

    return DEFAULT_FACTOR_YEAR


def normalize_amount_and_unit(amount: float, unit: str, category: str) -> Tuple[float, str, Optional[str]]:
    normalized_unit = normalize_unit(unit)
    if normalized_unit is None:
        raise ValueError("unit is required.")

    if normalized_unit == "miles":
        converted = convert_miles_to_km(amount)
        return converted, "vehicle_km", f"Converted miles to vehicle_km using 1 mile = 1.609344 km. Original amount: {amount} miles."

    return amount, normalized_unit, None


# ============================================================
# FACTOR TABLE MANAGEMENT
# ============================================================
REQUIRED_FACTOR_COLUMNS = {
    "factor_id",
    "factor_year",
    "category",
    "display_category",
    "scope",
    "activity_group",
    "item_name",
    "unit",
    "kgCO2e_per_unit",
    "factor_quality",
    "source_name",
}


def normalise_factor_table(factor_rows: Optional[List[Dict[str, Any]]] = None) -> pd.DataFrame:
    if not factor_rows:
        return FACTOR_TABLE.copy()

    custom = pd.DataFrame(factor_rows)
    if custom.empty:
        return FACTOR_TABLE.copy()

    # Accept common alternative names.
    rename_map = {
        "kg CO2e": "kgCO2e_per_unit",
        "kg_co2e": "kgCO2e_per_unit",
        "kg_co2e_per_unit": "kgCO2e_per_unit",
        "year": "factor_year",
        "source": "source_name",
        "data_source": "source_name",
    }
    custom = custom.rename(columns={k: v for k, v in rename_map.items() if k in custom.columns})

    for col in REQUIRED_FACTOR_COLUMNS:
        if col not in custom.columns:
            if col == "factor_id":
                custom[col] = [f"custom-factor-{i+1}" for i in range(len(custom))]
            elif col == "display_category":
                custom[col] = custom.get("category", "Unknown")
            elif col == "scope":
                custom[col] = "Unspecified"
            elif col == "activity_group":
                custom[col] = custom.get("category", "Unknown")
            elif col == "factor_quality":
                custom[col] = "custom"
            elif col == "source_name":
                custom[col] = "Custom factor table"
            else:
                custom[col] = None

    custom["factor_year"] = pd.to_numeric(custom["factor_year"], errors="coerce").fillna(DEFAULT_FACTOR_YEAR).astype(int)
    custom["kgCO2e_per_unit"] = pd.to_numeric(custom["kgCO2e_per_unit"], errors="coerce")
    custom = custom[custom["kgCO2e_per_unit"].notna()].copy()

    # Custom rows first so they override built-ins.
    combined = pd.concat([custom, FACTOR_TABLE.copy()], ignore_index=True)
    combined["_dedupe_key"] = (
        combined["factor_year"].astype(str).str.lower() + "|" +
        combined["category"].astype(str).str.lower() + "|" +
        combined["item_name"].astype(str).str.lower() + "|" +
        combined["unit"].astype(str).str.lower()
    )
    combined = combined.drop_duplicates("_dedupe_key", keep="first").drop(columns=["_dedupe_key"])
    return combined.reset_index(drop=True)


def _candidate_years(year: int, factor_table: pd.DataFrame) -> List[int]:
    available = sorted(set(pd.to_numeric(factor_table["factor_year"], errors="coerce").dropna().astype(int)), reverse=True)
    years = [year]
    if DEFAULT_FACTOR_YEAR not in years:
        years.append(DEFAULT_FACTOR_YEAR)
    for y in available:
        if y not in years:
            years.append(y)
    return years


def match_factor(
    category: str,
    item_name: str,
    unit: str,
    factor_year: int,
    factor_table: pd.DataFrame,
    allow_year_fallback: bool = True,
    allow_generic_fallback: bool = True,
) -> Tuple[pd.Series, str, str]:
    category_key = category.strip().lower()
    item_key = item_name.strip().lower()
    unit_key = unit.strip().lower()

    candidate_years = _candidate_years(factor_year, factor_table) if allow_year_fallback else [factor_year]

    # exact item/unit/year first
    for year in candidate_years:
        subset = factor_table[
            (factor_table["factor_year"].astype(int) == int(year)) &
            (factor_table["category"].astype(str).str.lower() == category_key) &
            (factor_table["item_name"].astype(str).str.lower() == item_key) &
            (factor_table["unit"].astype(str).str.lower() == unit_key)
        ]
        if not subset.empty:
            if year == factor_year:
                return subset.iloc[0], "exact", "Exact category, item, unit, and factor-year match."
            return subset.iloc[0], "year_fallback", f"Requested factor year {factor_year}; used available year {year}."

    if allow_generic_fallback:
        # generic same category/unit fallback
        for year in candidate_years:
            subset = factor_table[
                (factor_table["factor_year"].astype(int) == int(year)) &
                (factor_table["category"].astype(str).str.lower() == category_key) &
                (factor_table["unit"].astype(str).str.lower() == unit_key)
            ]
            if not subset.empty:
                return subset.iloc[0], "generic_category_unit_fallback", (
                    f"No exact item match for '{item_name}'. Used category/unit factor "
                    f"'{subset.iloc[0].get('item_name')}' for year {year}."
                )

    raise ValueError(
        f"No matching factor found for category='{category}', item_name='{item_name}', "
        f"unit='{unit}', factor_year={factor_year}."
    )


# ============================================================
# INFERENCE / ACTIVITY NORMALISATION
# ============================================================
def infer_canonical_category(activity: Dict[str, Any]) -> Dict[str, Any]:
    raw_category = safe_text(activity.get("category"))
    candidates = [
        ("category", activity.get("category")),
        ("item_name", activity.get("item_name")),
        ("fuel", activity.get("fuel")),
        ("sub_category", activity.get("sub_category")),
        ("source_section", activity.get("source_section")),
        ("raw_row_label", activity.get("raw_row_label")),
        ("notes", activity.get("notes")),
    ]

    for source, value in candidates:
        mapped = normalize_category(value)
        if mapped:
            return {
                "raw_category": raw_category or safe_text(value),
                "canonical_category": mapped,
                "mapping_source": source if source == "category" else "inferred",
                "mapping_confidence": "high" if source == "category" else "medium",
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
        activity.get("country"),
        activity.get("sub_category"),
        activity.get("source_section"),
        activity.get("raw_row_label"),
        activity.get("notes"),
        activity.get("category"),
    ]

    for value in candidates:
        item = normalize_item(value)
        if not item:
            continue
        if item.lower() in {"fuel", "liquid fuel", "business travel", "travel", "waste", "water", "electricity"}:
            continue
        if canonical_category == "electricity" and item.lower().startswith("ev charging"):
            return "Electricity: UK"
        return item

    defaults = {
        "fuel": "Natural gas",
        "liquid fuel": "Diesel",
        "electricity": "Electricity: UK",
        "water": "Water supply",
        "business travel": "Generic travel spend",
        "waste": "General waste",
    }
    return defaults.get(canonical_category, "Unknown")


def infer_scope_override(activity: Dict[str, Any]) -> Optional[str]:
    scope = safe_text(first_non_blank(activity.get("scope_override"), activity.get("scope")))
    if not scope:
        return None
    s = scope.lower().replace(" ", "")
    if s in {"scope1", "s1"}:
        return "Scope 1"
    if s in {"scope2", "s2"}:
        return "Scope 2"
    if s in {"scope3", "s3"}:
        return "Scope 3"
    return scope


# ============================================================
# WORKBOOK PARSER
# ============================================================
def records_to_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records).reset_index(drop=True)

    # If columns are col_1, col_2... keep order and make numeric columns.
    def col_num(c: Any) -> int:
        m = re.search(r"(\d+)$", str(c))
        return int(m.group(1)) if m else 999999

    ordered = sorted(df.columns, key=col_num)
    df = df[ordered]
    df.columns = list(range(df.shape[1]))
    return df


def preprocess_uploaded_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()
    df_clean.columns = [str(c).strip().lower().replace("\n", " ").replace("\r", " ") for c in df_clean.columns]

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
    if "fuel" in df_clean.columns:
        df_clean["fuel"] = df_clean["fuel"].apply(lambda x: normalize_item(x) if isinstance(x, str) else x)

    return df_clean.where(pd.notnull(df_clean), None)


def looks_like_canonical_activity_df(df: pd.DataFrame) -> bool:
    cols = {str(c).lower() for c in df.columns}
    return {"category", "amount", "unit"}.issubset(cols)


def find_matching_cells(df: pd.DataFrame, pattern: str) -> List[Tuple[int, int, str]]:
    matches = []
    regex = re.compile(pattern, flags=re.IGNORECASE)
    for r in range(df.shape[0]):
        for c in range(df.shape[1]):
            text = safe_text(df.iat[r, c])
            if text and regex.search(text):
                matches.append((r, c, text))
    return matches


def detect_workbook_sections(df: pd.DataFrame) -> Dict[str, List[Tuple[int, int, str]]]:
    return {
        "electricity": find_matching_cells(df, r"\belectricity\s+usage\b|\belectricity\b"),
        "gas": find_matching_cells(df, r"\bgas\s+charges\b|\bnatural\s+gas\b"),
        "fuel_litres": find_matching_cells(df, r"total\s+litres\s+for\s+fuel|fuel\s+litres|diesel\s+litres"),
        "adblue": find_matching_cells(df, r"\bad\s*blue\b|\badblue\b"),
        "ev_charging": find_matching_cells(df, r"electric\s+used\s+by\s+ev|ev\s+charging"),
        "business_miles": find_matching_cells(df, r"\bbusiness\s+miles\b|\bmileage\b"),
        "vehicles": find_matching_cells(df, r"\bvehicles\b|\bvehicle\s+registration\b|\bregistration\b"),
        "water": find_matching_cells(df, r"\bwater\s+(usage|supply|charges)\b"),
        "waste": find_matching_cells(df, r"\bwaste\b|\brecycling\b|\blandfill\b"),
    }


def append_parser_warning(diag: Dict[str, Any], message: str) -> None:
    diag.setdefault("warnings", [])
    if message not in diag["warnings"]:
        diag["warnings"].append(message)


def build_normalized_activity(
    category: str,
    amount: Any,
    unit: str,
    row_index_original: int,
    source_section: str,
    reporting_period: Optional[str] = None,
    **extra: Any,
) -> Dict[str, Any]:
    record = {
        "category": category,
        "amount": amount,
        "unit": unit,
        "row_index_original": row_index_original,
        "source_section": source_section,
        "reporting_period": reporting_period,
    }
    record.update(extra)
    return record


def parse_vehicle_registry(df_raw: pd.DataFrame, diag: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    registry: Dict[str, Dict[str, Any]] = {}

    # Search rows for registration-like values rather than hardcoding one layout only.
    reg_pattern = re.compile(r"\b[A-Z]{2}\d{2}\s?[A-Z]{3}\b", flags=re.IGNORECASE)

    for r in range(df_raw.shape[0]):
        row_texts = [safe_text(df_raw.iat[r, c]) for c in range(df_raw.shape[1])]
        row_joined = " ".join([x for x in row_texts if x])
        regs = reg_pattern.findall(row_joined)
        if not regs:
            continue

        for reg in regs:
            reg_clean = reg.upper().replace(" ", "")
            vehicle_text = row_joined
            vehicle_class = classify_vehicle_class(vehicle_text)
            # Heuristic driver: text immediately before vehicle/model columns is often a name.
            driver_name = None
            for txt in row_texts:
                if txt and txt.upper().replace(" ", "") == reg_clean:
                    break
                if txt and not looks_like_number(txt) and not looks_like_date(txt) and len(txt.split()) <= 4:
                    driver_name = txt

            registry[reg_clean] = {
                "vehicle_registration": reg_clean,
                "raw_vehicle": vehicle_text[:250],
                "vehicle_class": vehicle_class,
                "driver_name": driver_name,
                "source": "vehicle_registry_parser",
            }

    diag["fleet_registry"] = list(registry.values())[:100]
    diag["fleet_registry_count"] = len(registry)
    return registry


def _find_numeric_near(df_raw: pd.DataFrame, row_idx: int, start_col: int, offsets: Iterable[int]) -> Optional[Tuple[float, int]]:
    for offset in offsets:
        col = start_col + offset
        if 0 <= col < df_raw.shape[1] and looks_like_number(df_raw.iat[row_idx, col]):
            value = safe_float(df_raw.iat[row_idx, col], default=-1)
            if value >= 0:
                return value, col
    return None


def parse_electricity_sections(df_raw: pd.DataFrame, sections: List[Tuple[int, int, str]], diag: Dict[str, Any], opts: Dict[str, Any]) -> List[Dict[str, Any]]:
    records = []
    for title_row, title_col, title_text in sections[:5]:
        parsed = 0
        for r in range(title_row + 1, min(df_raw.shape[0], title_row + 60)):
            label = safe_text(df_raw.iat[r, title_col]) if title_col < df_raw.shape[1] else None
            if label and label.lower() == "total":
                break
            if label and re.search(r"gas\s+charges|fuel\s+litres|business\s+miles", label, flags=re.I):
                break
            dt = parse_date(label)
            if dt is None:
                continue

            found = _find_numeric_near(df_raw, r, title_col, [4, 3, 5, 2, 1])
            if not found:
                continue
            amount, amount_col = found
            if amount <= 0:
                continue

            records.append(build_normalized_activity(
                category="electricity",
                amount=amount,
                unit="kWh",
                row_index_original=r + 1,
                source_section=title_text,
                reporting_period=dt.strftime("%Y-%m"),
                source_file=opts.get("source_file"),
                source_sheet=opts.get("source_sheet"),
                source_workbook=opts.get("source_workbook"),
                country=opts.get("electricity_country", DEFAULT_COUNTRY),
                item_name=opts.get("electricity_country", DEFAULT_COUNTRY),
                period_start=serialise_timestamp(dt),
                period_end=serialise_timestamp(dt),
                raw_column=f"col_{amount_col + 1}",
                invoice_reference=safe_text(df_raw.iat[r, title_col + 1]) if title_col + 1 < df_raw.shape[1] else None,
                notes="Parsed from workbook electricity block.",
            ))
            parsed += 1
        diag.setdefault("sections_parsed", []).append({"section": title_text, "record_count": parsed, "calculation_path": "electricity_kwh"})
    return records


def parse_gas_sections(df_raw: pd.DataFrame, sections: List[Tuple[int, int, str]], diag: Dict[str, Any], opts: Dict[str, Any]) -> List[Dict[str, Any]]:
    records = []
    gas_unit = opts.get("gas_unit", "kWh (Gross CV)")
    for title_row, title_col, title_text in sections[:5]:
        parsed = 0
        for r in range(title_row + 1, min(df_raw.shape[0], title_row + 60)):
            period_value = df_raw.iat[r, title_col] if title_col < df_raw.shape[1] else None
            period_text = safe_text(period_value)
            if period_text and period_text.lower() == "total":
                break
            if period_text and re.search(r"electricity\s+usage|fuel\s+litres|business\s+miles", period_text, flags=re.I):
                break

            start, end = parse_period_bounds(period_value)
            if start is None and end is None:
                continue

            found = _find_numeric_near(df_raw, r, title_col, [2, 3, 4, 1])
            if not found:
                continue
            amount, amount_col = found
            if amount <= 0:
                continue

            dt_for_period = end or start
            records.append(build_normalized_activity(
                category="fuel",
                amount=amount,
                unit=gas_unit,
                row_index_original=r + 1,
                source_section=title_text,
                reporting_period=dt_for_period.strftime("%Y-%m") if dt_for_period is not None else period_text,
                source_file=opts.get("source_file"),
                source_sheet=opts.get("source_sheet"),
                source_workbook=opts.get("source_workbook"),
                fuel="Natural gas",
                item_name="Natural gas",
                period_start=serialise_timestamp(start),
                period_end=serialise_timestamp(end),
                raw_column=f"col_{amount_col + 1}",
                invoice_reference=safe_text(df_raw.iat[r, title_col + 1]) if title_col + 1 < df_raw.shape[1] else None,
                notes="Parsed from workbook gas block.",
            ))
            parsed += 1
        diag.setdefault("sections_parsed", []).append({"section": title_text, "record_count": parsed, "calculation_path": "gas_kwh"})
    return records


def parse_fuel_litres_sections(df_raw: pd.DataFrame, sections: List[Tuple[int, int, str]], diag: Dict[str, Any], opts: Dict[str, Any]) -> List[Dict[str, Any]]:
    records = []
    include = bool(opts.get("include_fleet_fuel", True))
    default_fuel_type = opts.get("fleet_fuel_type", "Diesel")
    for title_row, title_col, title_text in sections[:5]:
        parsed = 0
        for r in range(title_row + 1, min(df_raw.shape[0], title_row + 60)):
            date_value = df_raw.iat[r, title_col] if title_col < df_raw.shape[1] else None
            text = safe_text(date_value)
            if text and text.lower() == "total":
                break
            dt = parse_date(date_value)
            if dt is None:
                continue

            found = _find_numeric_near(df_raw, r, title_col, [1, 2, 3])
            if not found:
                continue
            litres, amount_col = found
            if litres <= 0:
                continue

            activity = build_normalized_activity(
                category="liquid fuel",
                amount=litres,
                unit="litres",
                row_index_original=r + 1,
                source_section=title_text,
                reporting_period=dt.strftime("%Y-%m"),
                source_file=opts.get("source_file"),
                source_sheet=opts.get("source_sheet"),
                source_workbook=opts.get("source_workbook"),
                fuel=default_fuel_type,
                item_name=default_fuel_type,
                period_start=serialise_timestamp(dt),
                period_end=serialise_timestamp(dt),
                raw_column=f"col_{amount_col + 1}",
                notes="Parsed from workbook fleet fuel block.",
            )
            if include:
                records.append(activity)
                parsed += 1
            else:
                diag.setdefault("excluded_sections", []).append({
                    "section": title_text,
                    "row_index_original": r + 1,
                    "reason": "Fleet fuel block detected but excluded because include_fleet_fuel=False.",
                })
        diag.setdefault("sections_parsed", []).append({"section": title_text, "record_count": parsed, "calculation_path": "liquid_fuel_litres" if include else "liquid_fuel_excluded"})
    return records


def parse_business_miles(df_raw: pd.DataFrame, sections: List[Tuple[int, int, str]], diag: Dict[str, Any], opts: Dict[str, Any], vehicle_registry: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    records = []
    include_mileage = bool(opts.get("include_business_mileage", False))
    include_when_fuel_present = bool(opts.get("include_business_mileage_when_fuel_present", False))
    workbook_has_fuel = any(x.get("calculation_path") == "liquid_fuel_litres" and x.get("record_count", 0) > 0 for x in diag.get("sections_parsed", []))

    for title_row, title_col, title_text in sections[:5]:
        parsed = 0
        # Most client sheets put driver names across header row and miles one row below.
        value_rows = [title_row + 1, title_row + 2]
        for c in range(title_col + 1, min(df_raw.shape[1], title_col + 20)):
            driver_name = safe_text(df_raw.iat[title_row, c]) if title_row < df_raw.shape[0] else None
            if not driver_name or driver_name.lower() in {"total", "business miles"}:
                continue

            miles = None
            row_used = None
            for vr in value_rows:
                if vr < df_raw.shape[0] and looks_like_number(df_raw.iat[vr, c]):
                    miles = safe_float(df_raw.iat[vr, c], default=-1)
                    row_used = vr
                    break
            if miles is None or miles < 0:
                continue

            vehicle_record = None
            for item in vehicle_registry.values():
                if safe_text(item.get("driver_name")) and safe_text(item.get("driver_name")).lower() == driver_name.lower():
                    vehicle_record = item
                    break

            activity = build_normalized_activity(
                category="business travel",
                amount=miles,
                unit="miles",
                row_index_original=(row_used or title_row) + 1,
                source_section=title_text,
                reporting_period=opts.get("default_business_miles_period"),
                source_file=opts.get("source_file"),
                source_sheet=opts.get("source_sheet"),
                source_workbook=opts.get("source_workbook"),
                item_name="Taxi / car",
                driver_name=driver_name,
                raw_vehicle=(vehicle_record or {}).get("raw_vehicle"),
                vehicle_class=(vehicle_record or {}).get("vehicle_class"),
                vehicle_registration=(vehicle_record or {}).get("vehicle_registration"),
                raw_column=f"col_{c + 1}",
                notes="Parsed from workbook business miles block.",
            )

            override_table = opts.get("vehicle_factor_overrides", {}) or {}
            override = override_table.get(activity.get("vehicle_class")) or override_table.get(driver_name)
            if override:
                activity.update({
                    "emission_factor_override": override.get("kg_co2e_per_unit"),
                    "factor_unit_override": override.get("unit", "vehicle_km"),
                    "factor_name_override": override.get("factor_name") or f"{activity.get('vehicle_class') or driver_name} mileage factor",
                    "factor_description_override": override.get("factor_description") or "Vehicle-specific factor supplied at runtime.",
                    "factor_quality_override": override.get("factor_quality", "runtime_override"),
                    "factor_year_override": override.get("factor_year", DEFAULT_FACTOR_YEAR),
                    "data_source_override": override.get("data_source", "Runtime vehicle factor override"),
                    "scope_override": override.get("scope", "Scope 3"),
                })

            should_exclude = (not include_mileage) or (workbook_has_fuel and not include_when_fuel_present)
            if should_exclude:
                diag.setdefault("excluded_sections", []).append({
                    "section": title_text,
                    "driver_name": driver_name,
                    "vehicle_class": activity.get("vehicle_class"),
                    "vehicle_registration": activity.get("vehicle_registration"),
                    "reason": "Business mileage excluded to avoid double counting with direct fleet fuel or because include_business_mileage=False.",
                })
            else:
                records.append(activity)
                parsed += 1

        diag.setdefault("sections_parsed", []).append({"section": title_text, "record_count": parsed, "calculation_path": "business_mileage" if include_mileage else "business_mileage_excluded"})
    return records


def parse_ev_charging(df_raw: pd.DataFrame, sections: List[Tuple[int, int, str]], diag: Dict[str, Any], opts: Dict[str, Any]) -> List[Dict[str, Any]]:
    records = []
    include = bool(opts.get("include_ev_charging_submeter", False))
    for title_row, title_col, title_text in sections[:5]:
        totals_preview = []
        parsed = 0
        # Look for date rows and sum numeric columns to the right.
        candidate_cols = range(title_col + 1, min(df_raw.shape[1], title_col + 20))
        for c in candidate_cols:
            header = None
            for hr in range(title_row, min(df_raw.shape[0], title_row + 4)):
                if safe_text(df_raw.iat[hr, c]):
                    header = safe_text(df_raw.iat[hr, c])
                    break
            if not header:
                continue

            total = 0.0
            count = 0
            for r in range(title_row + 1, min(df_raw.shape[0], title_row + 40)):
                if parse_date(df_raw.iat[r, title_col]) is None:
                    continue
                if looks_like_number(df_raw.iat[r, c]):
                    total += safe_float(df_raw.iat[r, c], 0.0)
                    count += 1
            if count == 0:
                continue
            totals_preview.append({"label": header, "kwh": round(total, 3)})
            if include and total > 0:
                records.append(build_normalized_activity(
                    category="electricity",
                    amount=total,
                    unit="kWh",
                    row_index_original=title_row + 1,
                    source_section=f"{title_text} / {header}",
                    reporting_period=opts.get("default_ev_charging_period"),
                    source_file=opts.get("source_file"),
                    source_sheet=opts.get("source_sheet"),
                    source_workbook=opts.get("source_workbook"),
                    item_name="Electricity: UK",
                    country=opts.get("electricity_country", DEFAULT_COUNTRY),
                    raw_column=f"col_{c + 1}",
                    notes="Parsed from EV charging sub-meter block.",
                ))
                parsed += 1

        if totals_preview:
            diag.setdefault("ev_charging_preview", []).extend(totals_preview[:20])
        if totals_preview and not include:
            append_parser_warning(diag, "EV charging block detected and excluded by default to avoid double counting against main electricity unless include_ev_charging_submeter=True.")
        diag.setdefault("sections_parsed", []).append({"section": title_text, "record_count": parsed, "calculation_path": "ev_charging_submeter" if include else "ev_charging_excluded"})
    return records


def parse_energy_workbook_records(records: List[Dict[str, Any]], parser_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    opts = parser_options or {}
    df_raw = records_to_dataframe(records)
    diag: Dict[str, Any] = {
        "parser_used": "workbook_block_parser",
        "input_shape": {"rows": int(df_raw.shape[0]), "cols": int(df_raw.shape[1])},
        "sections_detected": {},
        "sections_parsed": [],
        "excluded_sections": [],
        "warnings": [],
        "source_file": opts.get("source_file"),
        "source_sheet": opts.get("source_sheet"),
    }

    if df_raw.empty:
        append_parser_warning(diag, "Workbook records were empty.")
        return {"activities": [], "parser_diagnostics": diag}

    sections = detect_workbook_sections(df_raw)
    diag["sections_detected"] = {
        key: [{"row": r + 1, "col": c + 1, "label": text} for r, c, text in vals[:10]]
        for key, vals in sections.items() if vals
    }

    vehicle_registry = parse_vehicle_registry(df_raw, diag)

    activities: List[Dict[str, Any]] = []
    activities.extend(parse_electricity_sections(df_raw, sections.get("electricity", []), diag, opts))
    activities.extend(parse_gas_sections(df_raw, sections.get("gas", []), diag, opts))
    activities.extend(parse_fuel_litres_sections(df_raw, sections.get("fuel_litres", []), diag, opts))
    activities.extend(parse_ev_charging(df_raw, sections.get("ev_charging", []), diag, opts))
    activities.extend(parse_business_miles(df_raw, sections.get("business_miles", []), diag, opts, vehicle_registry))

    if sections.get("adblue"):
        append_parser_warning(diag, "AdBlue section detected. Default treatment is disclosure-only unless a confirmed factor override is supplied.")

    if not activities:
        append_parser_warning(diag, "No calculable activities were extracted. Check sheet layout, labels, parser options, or use a mapped upload template.")

    diag["extracted_activity_count"] = len(activities)
    return {"activities": activities, "parser_diagnostics": diag}


def prepare_activities_for_engine(activity_list: List[Dict[str, Any]], parser_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    opts = parser_options or {}
    if not activity_list:
        return {"activities": [], "parser_diagnostics": {"parser_used": "none", "warnings": ["No activities were supplied."], "extracted_activity_count": 0}}

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
                "source_file": opts.get("source_file"),
                "source_sheet": opts.get("source_sheet"),
            },
        }

    return parse_energy_workbook_records(activity_list, opts)


# ============================================================
# CALCULATION
# ============================================================
def classify_calculation_basis(mapping_source: Optional[str], factor_quality: str, match_type: str) -> str:
    fq = str(factor_quality or "").lower()
    if fq == "disclosure_only_placeholder":
        return "disclosure_only"
    if "placeholder" in fq:
        return "estimated"
    if fq in {"custom", "runtime_override", "client_override"}:
        return "estimated"
    if "official" in fq and mapping_source == "category" and match_type == "exact":
        return "exact"
    return "estimated"


def calculate_with_runtime_override(activity: Dict[str, Any], mapping: Dict[str, Any]) -> pd.DataFrame:
    amount = validate_amount(activity.get("amount"))
    unit = validate_text_input(activity.get("unit"), "unit")
    canonical_category = mapping["canonical_category"] or "business travel"
    normalized_amount, normalized_unit, conversion_note = normalize_amount_and_unit(amount, unit, canonical_category)

    factor_unit = normalize_unit(activity.get("factor_unit_override")) or normalized_unit
    if normalized_unit == "vehicle_km" and factor_unit == "vehicle_km":
        amount_for_factor = normalized_amount
    else:
        amount_for_factor = amount

    factor = validate_factor_value(activity.get("emission_factor_override"), "emission_factor_override")
    emissions_kg = amount_for_factor * factor
    factor_year = validate_year(first_non_blank(activity.get("factor_year_override"), activity.get("factor_year"), activity.get("year")))

    factor_quality = safe_text(activity.get("factor_quality_override")) or "runtime_override"
    scope = infer_scope_override(activity) or ("Scope 3" if canonical_category == "business travel" else "Unspecified")
    category_display = {
        "business travel": "Business Travel",
        "liquid fuel": "Liquid Fuel",
        "fuel": "Fuel",
        "electricity": "Electricity",
        "water": "Water",
        "waste": "Waste",
    }.get(canonical_category, canonical_category.title())

    formula = f"{amount_for_factor} {factor_unit} × {factor} kgCO2e/{factor_unit} = {emissions_kg} kgCO2e"

    row = {
        "category": category_display,
        "canonical_category": canonical_category,
        "sub_category": safe_text(activity.get("vehicle_class")) or "Runtime override",
        "item_name": safe_text(first_non_blank(activity.get("item_name"), activity.get("raw_vehicle"))) or category_display,
        "scope": scope,
        "input_amount": amount,
        "input_unit": unit,
        "normalized_amount": amount_for_factor,
        "normalized_unit": factor_unit,
        "unit_conversion_note": conversion_note,
        "emission_factor_kgCO2e_per_unit": factor,
        "factor_year_requested": factor_year,
        "factor_year": factor_year,
        "emissions_kgCO2e": emissions_kg,
        "emissions_tCO2e": emissions_kg / 1000.0,
        "calculation_formula": formula,
        "factor_id": safe_text(activity.get("factor_id_override")) or "runtime-override",
        "factor_name": safe_text(activity.get("factor_name_override")) or "Runtime factor override",
        "factor_description": safe_text(activity.get("factor_description_override")) or "Factor supplied at runtime.",
        "factor_source": safe_text(activity.get("data_source_override")) or "Runtime override",
        "data_source": safe_text(activity.get("data_source_override")) or "Runtime override",
        "factor_source_file": safe_text(activity.get("factor_source_file_override")),
        "factor_source_sheet": safe_text(activity.get("factor_source_sheet_override")),
        "factor_source_row": activity.get("factor_source_row_override"),
        "factor_quality": factor_quality,
        "factor_match_type": "runtime_override",
        "factor_match_note": "Emission factor supplied in request/activity row.",
        "original_category": mapping.get("raw_category"),
        "mapping_source": mapping.get("mapping_source"),
        "mapping_confidence": mapping.get("mapping_confidence"),
        "calculation_basis": classify_calculation_basis(mapping.get("mapping_source"), factor_quality, "runtime_override"),
        "engine_version": ENGINE_VERSION,
        "calculated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_row_hash": row_hash(activity),
    }
    for field in METADATA_FIELDS:
        row[field] = activity.get(field)
    return pd.DataFrame([row])


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

    row = {
        "category": factor_row.get("display_category", category.title()),
        "canonical_category": category,
        "sub_category": factor_row.get("activity_group"),
        "item_name": factor_row.get("item_name", item_name),
        "scope": infer_scope_override(activity) or factor_row.get("scope"),
        "input_amount": input_amount,
        "input_unit": input_unit,
        "normalized_amount": normalized_amount,
        "normalized_unit": normalized_unit,
        "unit_conversion_note": conversion_note,
        "emission_factor_kgCO2e_per_unit": factor,
        "factor_year_requested": factor_year_requested,
        "factor_year": int(factor_row.get("factor_year")),
        "emissions_kgCO2e": emissions_kg,
        "emissions_tCO2e": emissions_kg / 1000.0,
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
        row[field] = activity.get(field)

    return pd.DataFrame([row])


def calculate_emissions(activity: Dict[str, Any], factor_table: pd.DataFrame) -> pd.DataFrame:
    mapping = infer_canonical_category(activity)
    canonical_category = mapping["canonical_category"]

    if not canonical_category:
        raise ValueError(f"Unsupported or unmapped category: '{mapping.get('raw_category')}'. {SUPPORTED_CATEGORY_HINT}")

    if activity.get("emission_factor_override") is not None:
        return calculate_with_runtime_override(activity, mapping)

    input_unit = validate_text_input(activity.get("unit"), "unit")
    input_amount = validate_amount(activity.get("amount"))
    factor_year = get_factor_year(activity)
    item_name = infer_item_name(activity, canonical_category)

    normalized_amount, normalized_unit, conversion_note = normalize_amount_and_unit(input_amount, input_unit, canonical_category)

    # Treatment defaults
    if canonical_category == "fuel" and item_name == "Natural gas" and normalized_unit == "kWh":
        normalized_unit = "kWh (Gross CV)"
        conversion_note = (conversion_note + " " if conversion_note else "") + "Natural gas kWh defaulted to Gross CV."
    if canonical_category == "electricity":
        item_name = "Electricity: UK"
    if canonical_category == "liquid fuel" and item_name in {"Fuel", "Liquid Fuel", "fleet fuel"}:
        item_name = "Diesel"

    factor_row, match_type, match_note = match_factor(
        category=canonical_category,
        item_name=item_name,
        unit=normalized_unit,
        factor_year=factor_year,
        factor_table=factor_table,
        allow_year_fallback=True,
        allow_generic_fallback=True,
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
# ERROR GROUPING / DATA QUALITY
# ============================================================
def infer_issue_type_from_message(message: Any) -> str:
    msg = str(message or "").lower()
    if "unmapped category" in msg or "unsupported category" in msg:
        return "Unmapped category"
    if "amount is required" in msg:
        return "Missing amount"
    if "amount must be numeric" in msg:
        return "Non-numeric amount"
    if "amount cannot be negative" in msg:
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
    if issue_type in {"Unmapped category", "Factor matching failure", "Missing required field", "Missing unit", "Missing amount", "Non-numeric amount"}:
        return "high"
    if issue_type in {"Invalid year", "Negative amount", "Other validation issue"}:
        return "medium"
    if count >= 10:
        return "high"
    if count >= 3:
        return "medium"
    return "low"


def infer_issue_guidance(issue_type: str) -> str:
    return {
        "Unmapped category": "Standardise category labels or extend CATEGORY_ALIASES / transformer mapping rules.",
        "Missing amount": "Add a valid numeric amount for affected rows.",
        "Non-numeric amount": "Replace text/malformed amount values with clean numbers.",
        "Negative amount": "Check whether the quantity should be positive; handle credits/reversals explicitly.",
        "Missing unit": "Populate unit and ensure it maps to a supported unit.",
        "Factor matching failure": "Review category, item, unit, and factor year; add official factor rows if needed.",
        "Invalid year": "Use an integer reporting/factor year. Current configured target years are 2023, 2024, and 2025.",
        "Missing required field": "Complete required fields before submitting.",
        "Other validation issue": "Review affected rows and correct source data or parser mapping.",
    }.get(issue_type, "Review affected rows and correct source data.")


def group_errors(errors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for err in errors:
        issue_type = infer_issue_type_from_message(err.get("error"))
        item = grouped.setdefault(issue_type, {
            "title": issue_type,
            "count": 0,
            "severity": "low",
            "guidance": infer_issue_guidance(issue_type),
            "examples": [],
        })
        item["count"] += 1
        bits = []
        for key in ["source_file", "source_sheet", "source_section"]:
            if err.get(key):
                bits.append(f"{key}={err[key]}")
        if err.get("row_index_original") is not None:
            bits.append(f"row {err['row_index_original']}")
        elif err.get("row_index") is not None:
            bits.append(f"row {err['row_index']}")
        if err.get("error"):
            bits.append(str(err["error"]))
        example = " - ".join(bits)
        if example and len(item["examples"]) < 5:
            item["examples"].append(example)

    for issue_type, item in grouped.items():
        item["severity"] = infer_severity(issue_type, item["count"])

    severity_order = {"high": 0, "medium": 1, "low": 2}
    return sorted(grouped.values(), key=lambda x: (severity_order.get(x["severity"], 9), -x["count"], x["title"]))


def build_error_summary(errors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{"error": item["title"], "count": item["count"]} for item in group_errors(errors)]


def build_errors_preview(errors: List[Dict[str, Any]], max_items: int = 50) -> List[Dict[str, Any]]:
    return errors[:max_items]


def calculate_confidence_score(report_df: pd.DataFrame, data_quality: Dict[str, Any], parser_diagnostics: Dict[str, Any]) -> Dict[str, Any]:
    total_rows = int(data_quality.get("total_rows", 0) or 0)
    coverage = float(data_quality.get("coverage_percent", 0.0) or 0.0)
    inferred = int(data_quality.get("inferred_category_rows", 0) or 0)
    estimated = int(data_quality.get("estimated_rows", 0) or 0)
    placeholder = int(data_quality.get("starter_factor_rows", 0) or 0)
    fallback = int(data_quality.get("factor_fallback_rows", 0) or 0)
    disclosure = int(data_quality.get("disclosure_only_rows", 0) or 0)
    parser_warnings = len(parser_diagnostics.get("warnings", []) or [])
    excluded = len(parser_diagnostics.get("excluded_sections", []) or [])

    if total_rows <= 0:
        return {"score": 0, "label": "Low", "coverage_percent": 0.0, "notes": ["No extracted activity rows were available for calculation."]}

    score = 100.0
    score -= max(0.0, 100.0 - coverage) * 0.65
    score -= (inferred / total_rows) * 12.0
    score -= (estimated / total_rows) * 18.0
    score -= (placeholder / total_rows) * 25.0
    score -= (fallback / total_rows) * 10.0
    score -= (disclosure / total_rows) * 8.0
    score -= min(parser_warnings * 3.0, 15.0)
    score -= min(excluded * 1.0, 8.0)
    score = int(round(max(0.0, min(100.0, score)), 0))

    label = "High" if score >= 80 else "Moderate" if score >= 60 else "Low"
    notes: List[str] = []
    if coverage >= 95:
        notes.append("Most extracted rows were processed successfully.")
    elif coverage > 0:
        notes.append("Some extracted rows were excluded due to validation, parsing, or factor matching issues.")
    else:
        notes.append("No rows were successfully calculated.")

    if inferred:
        notes.append(f"{inferred} row(s) relied on inferred category mapping.")
    if estimated:
        notes.append(f"{estimated} row(s) used estimated or non-exact treatment.")
    if placeholder:
        notes.append(f"{placeholder} row(s) used starter/placeholder factor rows; replace these with official factor rows.")
    if fallback:
        notes.append(f"{fallback} row(s) used fallback factor matching.")
    if disclosure:
        notes.append(f"{disclosure} row(s) were disclosure-only and did not add calculated emissions.")
    if parser_warnings:
        notes.append(f"{parser_warnings} parser warning(s) should be reviewed.")
    if excluded:
        notes.append(f"{excluded} section/item(s) were intentionally excluded, commonly to avoid double counting.")

    return {"score": score, "label": label, "coverage_percent": coverage, "notes": notes}


# ============================================================
# BATCH PROCESSING
# ============================================================
def calculate_emissions_batch_safe(activity_list: List[Dict[str, Any]], factor_table: pd.DataFrame, parser_diagnostics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    parser_diagnostics = parser_diagnostics or {}
    results: List[pd.DataFrame] = []
    errors: List[Dict[str, Any]] = []
    mapping_rows: List[Dict[str, Any]] = []
    unmapped_categories: List[Any] = []

    for idx, activity in enumerate(activity_list):
        try:
            activity_data = dict(activity)
            mapping = infer_canonical_category(activity_data)
            mapping_rows.append({
                "row_index": idx,
                "original_category": mapping.get("raw_category"),
                "canonical_category": mapping.get("canonical_category"),
                "mapping_source": mapping.get("mapping_source"),
                "mapping_confidence": mapping.get("mapping_confidence"),
            })

            if not mapping.get("canonical_category"):
                unmapped_categories.append(mapping.get("raw_category"))
                raise ValueError(f"Unsupported or unmapped category: '{mapping.get('raw_category')}'. {SUPPORTED_CATEGORY_HINT}")

            result = calculate_emissions(activity_data, factor_table=factor_table).copy()
            result["row_index"] = idx
            results.append(result)

        except Exception as exc:
            errors.append({
                "row_index": idx,
                "row_index_original": activity.get("row_index_original", idx + 1),
                "source_file": activity.get("source_file"),
                "source_sheet": activity.get("source_sheet"),
                "source_section": activity.get("source_section"),
                "original_category": activity.get("category"),
                "input": activity,
                "error": str(exc),
            })

    final_report = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    total_kg = float(final_report["emissions_kgCO2e"].sum()) if not final_report.empty else 0.0
    total_t = total_kg / 1000.0

    total_rows = len(activity_list)
    successful_rows = len(final_report)
    errored_rows = len(errors)
    coverage = round((successful_rows / total_rows) * 100, 2) if total_rows else 0.0

    mapping_df = pd.DataFrame(mapping_rows)
    mapped_rows = int(mapping_df["canonical_category"].notna().sum()) if not mapping_df.empty else 0
    inferred_rows = int((mapping_df["mapping_source"] == "inferred").sum()) if not mapping_df.empty else 0

    unmapped_summary: List[Dict[str, Any]] = []
    if unmapped_categories:
        s = pd.Series(unmapped_categories, dtype="object").fillna("blank").value_counts().reset_index()
        s.columns = ["category", "count"]
        unmapped_summary = s.to_dict(orient="records")

    exact_rows = estimated_rows = starter_rows = official_rows = fallback_rows = disclosure_rows = 0
    if not final_report.empty:
        exact_rows = int((final_report["calculation_basis"] == "exact").sum()) if "calculation_basis" in final_report else 0
        estimated_rows = int((final_report["calculation_basis"] == "estimated").sum()) if "calculation_basis" in final_report else 0
        disclosure_rows = int((final_report["calculation_basis"] == "disclosure_only").sum()) if "calculation_basis" in final_report else 0
        starter_rows = int(final_report["factor_quality"].astype(str).str.contains("placeholder", case=False, na=False).sum()) if "factor_quality" in final_report else 0
        official_rows = int(final_report["factor_quality"].astype(str).str.contains("official", case=False, na=False).sum()) if "factor_quality" in final_report else 0
        fallback_rows = int((final_report["factor_match_type"] != "exact").sum()) if "factor_match_type" in final_report else 0

    data_quality = {
        "total_rows": total_rows,
        "successful_rows": successful_rows,
        "errored_rows": errored_rows,
        "coverage_percent": coverage,
        "mapped_category_rows": mapped_rows,
        "inferred_category_rows": inferred_rows,
        "exact_factor_rows": exact_rows,
        "estimated_rows": estimated_rows,
        "disclosure_only_rows": disclosure_rows,
        "official_factor_rows": official_rows,
        "starter_factor_rows": starter_rows,
        "factor_fallback_rows": fallback_rows,
        "valid_rows_percent": coverage,
        "invalid_rows_percent": round((errored_rows / total_rows) * 100, 2) if total_rows else 0.0,
        "extracted_activity_count": total_rows,
    }

    confidence = calculate_confidence_score(final_report, data_quality, parser_diagnostics)

    return {
        "report": final_report,
        "total_kgCO2e": total_kg,
        "total_tCO2e": total_t,
        "errors": errors,
        "data_quality": data_quality,
        "unmapped_categories": unmapped_summary,
        "confidence": confidence,
        "parser_diagnostics": parser_diagnostics,
    }


# ============================================================
# SUMMARIES / REPORT RESPONSE
# ============================================================
def _empty_df(cols: List[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=cols)


def summarize_report_by_scope(report_df: pd.DataFrame) -> pd.DataFrame:
    if report_df.empty:
        return _empty_df(["scope", "emissions_kgCO2e", "emissions_tCO2e", "percent_of_total"])
    df = report_df.groupby("scope", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]].sum().sort_values("emissions_kgCO2e", ascending=False)
    total = float(df["emissions_kgCO2e"].sum()) if not df.empty else 0.0
    df["percent_of_total"] = df["emissions_kgCO2e"].apply(lambda x: round((float(x) / total) * 100, 2) if total > 0 else 0.0)
    return df.reset_index(drop=True)


def summarize_report_by_scope_and_category(report_df: pd.DataFrame) -> pd.DataFrame:
    if report_df.empty:
        return _empty_df(["scope", "category", "emissions_kgCO2e", "emissions_tCO2e"])
    return report_df.groupby(["scope", "category"], as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]].sum().sort_values("emissions_kgCO2e", ascending=False).reset_index(drop=True)


def summarize_top_categories(report_df: pd.DataFrame, top_n: int = 5) -> List[Dict[str, Any]]:
    if report_df.empty:
        return []
    return report_df.groupby("category", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]].sum().sort_values("emissions_kgCO2e", ascending=False).head(top_n).to_dict(orient="records")


def summarize_top_sites(report_df: pd.DataFrame, top_n: int = 5) -> List[Dict[str, Any]]:
    if report_df.empty or "site_name" not in report_df.columns:
        return []
    working = report_df.copy()
    working["site_name"] = working["site_name"].fillna("Unspecified site")
    return working.groupby("site_name", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]].sum().sort_values("emissions_kgCO2e", ascending=False).head(top_n).to_dict(orient="records")


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
        "category", "canonical_category", "scope", "item_name", "vehicle_class", "vehicle_registration",
        "factor_id", "factor_name", "factor_description", "emission_factor_kgCO2e_per_unit",
        "normalized_unit", "factor_year", "factor_year_requested", "factor_source",
        "factor_source_file", "factor_source_sheet", "factor_source_row", "factor_quality",
        "factor_match_type", "factor_match_note", "calculation_basis", "source_file",
        "source_sheet", "source_section", "reporting_period",
    ]
    available = [c for c in cols if c in report_df.columns]
    out = report_df[available].drop_duplicates().reset_index(drop=True)
    sort_cols = [c for c in ["category", "item_name", "factor_year"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols).reset_index(drop=True)
    return out.to_dict(orient="records")


def build_analytics(report_df: pd.DataFrame) -> Dict[str, Any]:
    analytics: Dict[str, Any] = {"average_emissions_per_row_kgco2e": 0.0}
    if report_df.empty:
        return analytics
    analytics["average_emissions_per_row_kgco2e"] = round(float(report_df["emissions_kgCO2e"].mean()), 4)
    cats = summarize_top_categories(report_df, 5)
    if cats:
        analytics["top_category_by_kgco2e"] = cats[0]
        analytics["top_categories_by_kgco2e"] = cats
    sites = summarize_top_sites(report_df, 5)
    if sites:
        analytics["top_site_by_kgco2e"] = sites[0]
        analytics["top_sites_by_kgco2e"] = sites
    scope_df = summarize_report_by_scope(report_df)
    if not scope_df.empty:
        analytics["top_scope_by_kgco2e"] = scope_df.iloc[0].to_dict()
    return analytics


def build_plain_language_takeaways(report_df: pd.DataFrame, batch_result: Dict[str, Any], summary_scope: pd.DataFrame, analytics: Dict[str, Any]) -> List[str]:
    total_kg = float(batch_result.get("total_kgCO2e", 0.0) or 0.0)
    total_t = float(batch_result.get("total_tCO2e", 0.0) or 0.0)
    dq = batch_result.get("data_quality", {})
    confidence = batch_result.get("confidence", {})
    pdx = batch_result.get("parser_diagnostics", {})
    takeaways: List[str] = []

    if total_kg > 0:
        takeaways.append(f"Total reported emissions are {round(total_t, 2)} tCO2e ({round(total_kg, 2)} kgCO2e) across the processed dataset.")
    else:
        takeaways.append("No reportable emissions were calculated from the current input.")

    if not summary_scope.empty:
        top = summary_scope.iloc[0]
        takeaways.append(f"{top['scope']} is the largest contributor, representing {round(float(top['percent_of_total']), 2)}% of total emissions.")

    top_category = analytics.get("top_category_by_kgco2e")
    if top_category:
        takeaways.append(f"The highest-emitting category is {top_category.get('category')}, contributing {round(float(top_category.get('emissions_kgCO2e', 0.0)), 2)} kgCO2e.")

    coverage = dq.get("coverage_percent", 0.0)
    takeaways.append(f"Data processing coverage is {coverage}%, based on extracted activity rows successfully included in the calculation.")

    if pdx.get("parser_used") == "workbook_block_parser":
        takeaways.append(f"The workbook parser extracted {pdx.get('extracted_activity_count', dq.get('extracted_activity_count', 0))} calculable activity row(s) from a semi-structured source.")

    if confidence:
        takeaways.append(f"The current confidence score is {confidence.get('score', 0)}/100, rated {confidence.get('label', 'Low')}.")
    return takeaways[:6]


def build_actionable_insights(report_df: pd.DataFrame, batch_result: Dict[str, Any], summary_scope: pd.DataFrame, analytics: Dict[str, Any]) -> List[Dict[str, Any]]:
    dq = batch_result.get("data_quality", {})
    confidence = batch_result.get("confidence", {})
    pdx = batch_result.get("parser_diagnostics", {})
    unmapped = batch_result.get("unmapped_categories", [])
    insights: List[Dict[str, Any]] = []

    if dq.get("extracted_activity_count", dq.get("total_rows", 0)) == 0:
        insights.append({"title": "Parser review required", "body": "No calculable activity rows were extracted. Check workbook structure, section labels, and parser options before issuing a client report."})

    if not summary_scope.empty:
        top = summary_scope.iloc[0]
        insights.append({"title": "Largest emission driver", "body": f"{top['scope']} is currently the biggest contributor at {round(float(top['emissions_kgCO2e']), 2)} kgCO2e ({round(float(top['percent_of_total']), 2)}% of total emissions)."})

    top_category = analytics.get("top_category_by_kgco2e")
    if top_category:
        insights.append({"title": "Priority reduction opportunity", "body": f"The category with the greatest impact is {top_category.get('category')}. This is the first place to investigate for reductions and data-quality review."})

    if dq.get("coverage_percent", 0.0) < 90 and dq.get("total_rows", 0) > 0:
        insights.append({"title": "Coverage improvement needed", "body": f"Coverage is currently {dq.get('coverage_percent', 0.0)}%. Resolve parser warnings, validation errors, and factor-matching failures before relying on totals externally."})

    if dq.get("starter_factor_rows", 0) > 0:
        insights.append({"title": "Replace starter factors", "body": f"{dq.get('starter_factor_rows', 0)} row(s) used starter/placeholder factors. Replace these with official DEFRA/UK Government factor rows for audit-grade reporting."})

    if dq.get("factor_fallback_rows", 0) > 0:
        insights.append({"title": "Review fallback matches", "body": f"{dq.get('factor_fallback_rows', 0)} row(s) used fallback factor matching. These line items should be reviewed before client release."})

    if pdx.get("excluded_sections"):
        insights.append({"title": "Double-counting safeguards applied", "body": f"{len(pdx.get('excluded_sections', []))} item(s) were excluded, commonly to avoid double counting fleet fuel/mileage or main electricity/EV sub-meter data."})

    if unmapped:
        insights.append({"title": "Category mapping improvement needed", "body": f"Some categories remain unmapped. Example: '{unmapped[0].get('category')}' appears {unmapped[0].get('count', 0)} time(s)."})

    if confidence and confidence.get("score", 0) < 60:
        insights.append({"title": "Low-confidence result", "body": f"The current confidence score is {confidence.get('score', 0)}/100. Input cleanup, official factor loading, and parser review should be prioritised."})

    return insights[:8]


def build_methodology_summary(batch_result: Dict[str, Any]) -> Dict[str, Any]:
    dq = batch_result.get("data_quality", {})
    pdx = batch_result.get("parser_diagnostics", {})
    assumptions = [
        "Natural gas kWh defaults to Gross CV unless Net CV or Gross CV is explicitly supplied.",
        "Miles are converted to vehicle kilometres using 1 mile = 1.609344 km.",
        "Fleet fuel litres are included by default.",
        "Business mileage is excluded by default where direct fleet fuel is present unless explicitly enabled.",
        "EV charging sub-meter data is excluded by default unless explicitly enabled, to avoid double counting against main electricity.",
    ]
    if dq.get("starter_factor_rows", 0):
        assumptions.append("Some rows used starter placeholder factors and require official factor replacement before final reporting.")
    if pdx.get("parser_used") == "workbook_block_parser":
        assumptions.append("A workbook block parser was used to normalise semi-structured client data into activity rows.")

    return {
        "framework": "GHG Protocol aligned reporting structure using activity data multiplied by emissions factors.",
        "factor_basis": "Engine supports 2023, 2024, and 2025 factor-year selection. Built-in starter rows should be replaced or supplemented with official UK Government/DEFRA flat-file factor rows for audit use.",
        "calculation_logic": "Each valid row is mapped to a canonical category, normalised to a supported unit, matched to an emissions factor, multiplied by the activity quantity, and aggregated into CO2e outputs.",
        "assumptions": assumptions,
        "interpretation_notes": [
            "Review line-item factor transparency, parser diagnostics, excluded sections, and data-quality metrics alongside totals.",
            "Do not issue an external report if extracted activity count is zero, coverage is materially incomplete, or placeholder/fallback factor rows remain unresolved.",
        ],
        "engine_version": ENGINE_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "data_quality_snapshot": dq,
    }


def build_final_response(batch_result: Dict[str, Any]) -> Dict[str, Any]:
    report_df = batch_result["report"]
    summary_scope = summarize_report_by_scope(report_df)
    summary_scope_category = summarize_report_by_scope_and_category(report_df)
    analytics = build_analytics(report_df)

    return {
        "line_items": report_df.to_dict(orient="records"),
        "summary_by_scope_category": summary_scope_category.to_dict(orient="records"),
        "summary_by_scope": summary_scope.to_dict(orient="records"),
        "totals": {
            "total_kgCO2e": batch_result["total_kgCO2e"],
            "total_tCO2e": batch_result["total_tCO2e"],
        },
        "analytics": analytics,
        "top_categories_by_kgco2e": summarize_top_categories(report_df, 5),
        "top_sites_by_kgco2e": summarize_top_sites(report_df, 5),
        "plain_language_takeaways": build_plain_language_takeaways(report_df, batch_result, summary_scope, analytics),
        "actionable_insights": build_actionable_insights(report_df, batch_result, summary_scope, analytics),
        "confidence_score": batch_result.get("confidence", {}),
        "data_quality": batch_result.get("data_quality", {}),
        "parser_diagnostics": batch_result.get("parser_diagnostics", {}),
        "errors": batch_result["errors"],
        "errors_preview": build_errors_preview(batch_result["errors"], 50),
        "error_summary": build_error_summary(batch_result["errors"]),
        "issue_groups": group_errors(batch_result["errors"]),
        "unmapped_categories": batch_result.get("unmapped_categories", []),
        "factor_transparency": build_factor_transparency(report_df),
        "factor_quality_summary": summarize_factor_quality(report_df),
        "methodology_summary": build_methodology_summary(batch_result),
        "engine_version": ENGINE_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }


# ============================================================
# JSON SAFETY / PUBLIC ENTRYPOINT
# ============================================================
def make_json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [make_json_safe(v) for v in obj]
    if isinstance(obj, pd.DataFrame):
        return make_json_safe(obj.to_dict(orient="records"))
    if isinstance(obj, pd.Series):
        return make_json_safe(obj.to_dict())
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    try:
        if pd.isna(obj) and not isinstance(obj, str):
            return None
    except Exception:
        pass
    return obj


def run_emissions_engine(request_json: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(request_json, dict):
        raise ValueError("request_json must be a dictionary.")

    activities = request_json.get("activities", [])
    if not isinstance(activities, list):
        raise ValueError("activities must be a list.")

    parser_options = request_json.get("parser_options", {}) or {}
    if not isinstance(parser_options, dict):
        raise ValueError("parser_options must be a dictionary.")

    factor_rows = request_json.get("factor_rows")
    factor_table = normalise_factor_table(factor_rows if isinstance(factor_rows, list) else None)

    prepared = prepare_activities_for_engine(activities, parser_options=parser_options)
    normalized_activities = prepared.get("activities", [])
    parser_diagnostics = prepared.get("parser_diagnostics", {})

    batch_result = calculate_emissions_batch_safe(
        normalized_activities,
        factor_table=factor_table,
        parser_diagnostics=parser_diagnostics,
    )
    final_response = build_final_response(batch_result)
    return make_json_safe(final_response)

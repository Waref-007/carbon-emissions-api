from __future__ import annotations

import math
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


# =============================================================================
# PURPOSE
# =============================================================================
# This is a full replacement for the original engine.py.
#
# What this fixes:
# 1) Stops assuming every upload is a flat CSV with strict category/unit/amount columns.
# 2) Adds a workbook parser for semi-structured energy reports like the uploaded sample.
# 3) Supports DEFRA 2023, 2024, and 2025 factors for the categories used here.
# 4) Adds transparent factor logging, parser diagnostics, mapping preview, and validation.
# 5) Maps named vehicles to explicit DEFRA vehicle classes where possible.
#
# Notes on DEFRA usage:
# - DEFRA does not publish make/model-specific factors for every exact vehicle.
# - The correct defensible approach is: map the exact vehicle to the closest official DEFRA
#   class (for example Lower medium BEV, Executive PHEV, Dual purpose 4X4 diesel,
#   Articulated >33t, Rigid >17 tonnes) and log that mapping clearly.
#
# This engine is self-contained. It embeds the official factor values used here for 2023-2025.
# If you later want wider category coverage, extend FACTORS and VEHICLE_PROFILE_RULES.
# =============================================================================


# =============================================================================
# DEFRA FACTORS (embedded subset used by this engine)
# Source basis: UK Government GHG Conversion Factors flat files, years 2023/2024/2025.
# =============================================================================
FACTORS: Dict[int, Dict[str, Any]] = {
    2023: {
        "electricity_uk_kwh": 0.20707428859060403,
        "natural_gas_kwh_gross": 0.18292892617449666,
        "natural_gas_kwh_net": 0.2026714187919463,
        "natural_gas_m3": 2.038390310067114,
        "diesel_litres": 2.5120638845637586,
        "diesel_wtt_litres": 0.61101,
        "vehicle_distance": {
            "lower_medium_bev_miles": 0.07784943087248322,
            "lower_medium_phev_scope1_miles": 0.09618665637583894,
            "lower_medium_phev_scope2_miles": 0.03459225234899329,
            "upper_medium_phev_scope1_miles": 0.10157231946308726,
            "upper_medium_phev_scope2_miles": 0.035201144966442956,
            "executive_phev_scope1_miles": 0.10702558255033558,
            "executive_phev_scope2_miles": 0.033111144966442956,
            "dual_purpose_4x4_diesel_miles": 0.32500566308724826,
            "articulated_over_33t_avg_miles": 1.468762590604027,
            "rigid_over_17t_avg_miles": 1.568077855033557,
        },
    },
    2024: {
        "electricity_uk_kwh": 0.20705,
        "natural_gas_kwh_gross": 0.18290,
        "natural_gas_kwh_net": 0.20264,
        "natural_gas_m3": 2.04542,
        "diesel_litres": 2.51279,
        "diesel_wtt_litres": 0.61101,
        "vehicle_distance": {
            "lower_medium_bev_miles": 0.06824,
            "lower_medium_phev_scope1_miles": 0.12493,
            "lower_medium_phev_scope2_miles": 0.01702,
            "upper_medium_phev_scope1_miles": 0.13640,
            "upper_medium_phev_scope2_miles": 0.01820,
            "executive_phev_scope1_miles": 0.13837,
            "executive_phev_scope2_miles": 0.01504,
            "dual_purpose_4x4_diesel_miles": 0.31797,
            "articulated_over_33t_avg_miles": 1.46846,
            "rigid_over_17t_avg_miles": 1.57231,
        },
    },
    2025: {
        "electricity_uk_kwh": 0.17700,
        "natural_gas_kwh_gross": 0.18296,
        "natural_gas_kwh_net": 0.20270,
        "natural_gas_m3": 2.06672,
        "diesel_litres": 2.57082,
        "diesel_wtt_litres": 0.61101,
        "vehicle_distance": {
            "lower_medium_bev_miles": 0.05606,
            "lower_medium_phev_scope1_miles": 0.12044,
            "lower_medium_phev_scope2_miles": 0.01448,
            "upper_medium_phev_scope1_miles": 0.13047,
            "upper_medium_phev_scope2_miles": 0.01553,
            "executive_phev_scope1_miles": 0.13291,
            "executive_phev_scope2_miles": 0.01274,
            "dual_purpose_4x4_diesel_miles": 0.32145,
            "articulated_over_33t_avg_miles": 1.50346,
            "rigid_over_17t_avg_miles": 1.59575,
        },
    },
}


# =============================================================================
# VEHICLE MAPPING
# =============================================================================
# Each exact model is mapped to the closest official DEFRA class.
# This is the transparent way to address the client concern without inventing fake
# make/model factors that DEFRA does not publish.
VEHICLE_PROFILE_RULES: List[Dict[str, str]] = [
    {
        "match": "volkswagen id3",
        "vehicle_label": "Volkswagen ID3",
        "defra_class": "Lower medium battery electric vehicle",
        "factor_key": "lower_medium_bev_miles",
        "scope_split": "scope2_only",
    },
    {
        "match": "golf gte",
        "vehicle_label": "Volkswagen Golf GTE Plug-in Hybrid",
        "defra_class": "Lower medium plug-in hybrid electric vehicle",
        "factor_key": "lower_medium_phev",
        "scope_split": "scope1_plus_scope2",
    },
    {
        "match": "bmw 330e",
        "vehicle_label": "BMW 330e Plug-in Hybrid",
        "defra_class": "Executive plug-in hybrid electric vehicle",
        "factor_key": "executive_phev",
        "scope_split": "scope1_plus_scope2",
    },
    {
        "match": "ford ranger wildtrak",
        "vehicle_label": "Ford Ranger Wildtrak",
        "defra_class": "Dual purpose 4X4 diesel",
        "factor_key": "dual_purpose_4x4_diesel_miles",
        "scope_split": "scope1_only",
    },
    {
        "match": "actros",
        "vehicle_label": "Mercedes-Benz Actros",
        "defra_class": "Rigid >17 tonnes average laden",
        "factor_key": "rigid_over_17t_avg_miles",
        "scope_split": "scope1_only",
    },
    {
        "match": "dxf350 articulated",
        "vehicle_label": "DAF XF350 articulated unit",
        "defra_class": "Articulated >33t average laden",
        "factor_key": "articulated_over_33t_avg_miles",
        "scope_split": "scope1_only",
    },
    {
        "match": "44 ton",
        "vehicle_label": "44 ton articulated HGV",
        "defra_class": "Articulated >33t average laden",
        "factor_key": "articulated_over_33t_avg_miles",
        "scope_split": "scope1_only",
    },
    {
        "match": "26 ton",
        "vehicle_label": "26 ton rigid HGV",
        "defra_class": "Rigid >17 tonnes average laden",
        "factor_key": "rigid_over_17t_avg_miles",
        "scope_split": "scope1_only",
    },
]


# =============================================================================
# DATA STRUCTURES
# =============================================================================
@dataclass
class Activity:
    category: str
    amount: float
    unit: str
    factor_year: int
    scope_hint: Optional[str] = None
    item_name: Optional[str] = None
    sub_category: Optional[str] = None
    source_file: Optional[str] = None
    sheet_name: Optional[str] = None
    period_label: Optional[str] = None
    activity_date: Optional[str] = None
    row_index_original: Optional[int] = None
    notes: Optional[str] = None
    vehicle_name: Optional[str] = None
    registration: Optional[str] = None
    vehicle_profile: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


# =============================================================================
# BASIC HELPERS
# =============================================================================
def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    if isinstance(value, str):
        value = value.replace(",", "").replace("£", "").strip()
        if value == "":
            return default
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return default


def safe_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    return text or None


def clean_key(text: Any) -> str:
    return re.sub(r"\s+", " ", (safe_text(text) or "").strip().lower())


def parse_date_like(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    text = safe_text(value)
    if not text:
        return None

    for dayfirst in (False, True):
        try:
            return pd.to_datetime(text, dayfirst=dayfirst, errors="raise").to_pydatetime()
        except Exception:
            pass
    return None


def parse_period_start(period_label: str) -> Optional[datetime]:
    text = safe_text(period_label)
    if not text:
        return None
    parts = re.split(r"\s*-\s*", text)
    if not parts:
        return None
    return parse_date_like(parts[0])


def choose_factor_year(year: int) -> int:
    if year <= 2023:
        return 2023
    if year == 2024:
        return 2024
    return 2025


def make_json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [make_json_safe(v) for v in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


# =============================================================================
# FACTOR LOOKUPS
# =============================================================================
def get_factor(year: int, key: str) -> float:
    year = choose_factor_year(year)
    return float(FACTORS[year][key])


def get_vehicle_profile(vehicle_name: Optional[str], registration: Optional[str] = None) -> Optional[Dict[str, Any]]:
    haystack = f"{safe_text(vehicle_name) or ''} {safe_text(registration) or ''}".lower()
    if not haystack.strip():
        return None

    for rule in VEHICLE_PROFILE_RULES:
        if rule["match"] in haystack:
            return dict(rule)
    return None


# =============================================================================
# RESULT ROW BUILDERS
# =============================================================================
def result_row(
    *,
    category: str,
    scope: str,
    amount: float,
    unit: str,
    factor_year: int,
    factor: float,
    factor_name: str,
    factor_description: str,
    data_source: str,
    source_file: Optional[str] = None,
    sheet_name: Optional[str] = None,
    period_label: Optional[str] = None,
    activity_date: Optional[str] = None,
    row_index_original: Optional[int] = None,
    item_name: Optional[str] = None,
    sub_category: Optional[str] = None,
    notes: Optional[str] = None,
    vehicle_name: Optional[str] = None,
    registration: Optional[str] = None,
    mapping_confidence: str = "high",
    factor_quality: str = "official",
) -> Dict[str, Any]:
    emissions = float(amount) * float(factor)
    return {
        "category": category,
        "sub_category": sub_category or category,
        "item_name": item_name or category,
        "scope": scope,
        "input_amount": float(amount),
        "input_unit": unit,
        "normalized_unit": unit,
        "emission_factor_kgCO2e_per_unit": float(factor),
        "factor_year": int(factor_year),
        "emissions_kgCO2e": emissions,
        "emissions_tCO2e": emissions / 1000.0,
        "data_source": data_source,
        "factor_name": factor_name,
        "factor_description": factor_description,
        "factor_quality": factor_quality,
        "mapping_source": "parser" if source_file else "api",
        "mapping_confidence": mapping_confidence,
        "calculation_basis": "exact",
        "source_file": source_file,
        "sheet_name": sheet_name,
        "period_label": period_label,
        "activity_date": activity_date,
        "row_index_original": row_index_original,
        "notes": notes,
        "vehicle_name": vehicle_name,
        "registration": registration,
    }


# =============================================================================
# CATEGORY CALCULATORS
# =============================================================================
def calc_electricity(activity: Activity) -> List[Dict[str, Any]]:
    factor = get_factor(activity.factor_year, "electricity_uk_kwh")
    return [
        result_row(
            category="Electricity",
            scope="Scope 2",
            amount=activity.amount,
            unit="kWh",
            factor_year=activity.factor_year,
            factor=factor,
            factor_name="UK electricity / Electricity: UK / kWh",
            factor_description="DEFRA UK electricity location-based factor",
            data_source=f"DEFRA {activity.factor_year}",
            source_file=activity.source_file,
            sheet_name=activity.sheet_name,
            period_label=activity.period_label,
            activity_date=activity.activity_date,
            row_index_original=activity.row_index_original,
            item_name=activity.item_name or "Electricity: UK",
            sub_category=activity.sub_category or "Electricity generated",
            notes=activity.notes,
            vehicle_name=activity.vehicle_name,
            registration=activity.registration,
        )
    ]


def calc_gas(activity: Activity) -> List[Dict[str, Any]]:
    unit_key = {
        "kwh (gross cv)": "natural_gas_kwh_gross",
        "kwh (net cv)": "natural_gas_kwh_net",
        "cubic metres": "natural_gas_m3",
    }.get(clean_key(activity.unit), "natural_gas_kwh_gross")
    factor = get_factor(activity.factor_year, unit_key)
    return [
        result_row(
            category="Fuel",
            scope="Scope 1",
            amount=activity.amount,
            unit=activity.unit,
            factor_year=activity.factor_year,
            factor=factor,
            factor_name=f"Natural gas / {activity.unit}",
            factor_description="DEFRA natural gas combustion factor",
            data_source=f"DEFRA {activity.factor_year}",
            source_file=activity.source_file,
            sheet_name=activity.sheet_name,
            period_label=activity.period_label,
            activity_date=activity.activity_date,
            row_index_original=activity.row_index_original,
            item_name=activity.item_name or "Natural gas",
            sub_category=activity.sub_category or "Gaseous fuels",
            notes=activity.notes,
        )
    ]


def calc_diesel(activity: Activity) -> List[Dict[str, Any]]:
    combustion = get_factor(activity.factor_year, "diesel_litres")
    wtt = get_factor(activity.factor_year, "diesel_wtt_litres")
    return [
        result_row(
            category="Fuel",
            scope="Scope 1",
            amount=activity.amount,
            unit="litres",
            factor_year=activity.factor_year,
            factor=combustion,
            factor_name="Diesel (average biofuel blend) / litres",
            factor_description="DEFRA diesel combustion factor",
            data_source=f"DEFRA {activity.factor_year}",
            source_file=activity.source_file,
            sheet_name=activity.sheet_name,
            period_label=activity.period_label,
            activity_date=activity.activity_date,
            row_index_original=activity.row_index_original,
            item_name=activity.item_name or "Diesel (average biofuel blend)",
            sub_category=activity.sub_category or "Liquid fuels",
            notes=activity.notes,
        ),
        result_row(
            category="Fuel WTT",
            scope="Scope 3",
            amount=activity.amount,
            unit="litres",
            factor_year=activity.factor_year,
            factor=wtt,
            factor_name="Diesel WTT / litres",
            factor_description="DEFRA well-to-tank diesel factor",
            data_source=f"DEFRA {activity.factor_year}",
            source_file=activity.source_file,
            sheet_name=activity.sheet_name,
            period_label=activity.period_label,
            activity_date=activity.activity_date,
            row_index_original=activity.row_index_original,
            item_name="Diesel (average biofuel blend)",
            sub_category="WTT- fuels",
            notes=activity.notes,
        ),
    ]


def calc_vehicle_miles(activity: Activity) -> List[Dict[str, Any]]:
    profile = activity.vehicle_profile or {}
    factor_key = profile.get("factor_key")
    factor_year = choose_factor_year(activity.factor_year)
    vehicle_factors = FACTORS[factor_year]["vehicle_distance"]
    scope_split = profile.get("scope_split")
    if not factor_key:
        raise ValueError(f"No DEFRA vehicle profile mapping available for {activity.vehicle_name!r}")

    rows: List[Dict[str, Any]] = []
    if scope_split == "scope2_only":
        factor = float(vehicle_factors[factor_key])
        rows.append(
            result_row(
                category="Business travel",
                scope="Scope 2",
                amount=activity.amount,
                unit="miles",
                factor_year=factor_year,
                factor=factor,
                factor_name=f"{profile['defra_class']} / miles",
                factor_description="DEFRA UK electricity for EVs distance factor",
                data_source=f"DEFRA {factor_year}",
                source_file=activity.source_file,
                sheet_name=activity.sheet_name,
                period_label=activity.period_label,
                activity_date=activity.activity_date,
                row_index_original=activity.row_index_original,
                item_name=activity.vehicle_name or profile.get("vehicle_label"),
                sub_category=profile["defra_class"],
                notes=activity.notes,
                vehicle_name=activity.vehicle_name,
                registration=activity.registration,
            )
        )
        return rows

    if scope_split == "scope1_only":
        factor = float(vehicle_factors[factor_key])
        rows.append(
            result_row(
                category="Business travel",
                scope="Scope 1",
                amount=activity.amount,
                unit="miles",
                factor_year=factor_year,
                factor=factor,
                factor_name=f"{profile['defra_class']} / miles",
                factor_description="DEFRA delivery or passenger vehicle distance factor",
                data_source=f"DEFRA {factor_year}",
                source_file=activity.source_file,
                sheet_name=activity.sheet_name,
                period_label=activity.period_label,
                activity_date=activity.activity_date,
                row_index_original=activity.row_index_original,
                item_name=activity.vehicle_name or profile.get("vehicle_label"),
                sub_category=profile["defra_class"],
                notes=activity.notes,
                vehicle_name=activity.vehicle_name,
                registration=activity.registration,
            )
        )
        return rows

    if scope_split == "scope1_plus_scope2":
        scope1 = float(vehicle_factors[f"{factor_key}_scope1_miles"])
        scope2 = float(vehicle_factors[f"{factor_key}_scope2_miles"])
        rows.append(
            result_row(
                category="Business travel",
                scope="Scope 1",
                amount=activity.amount,
                unit="miles",
                factor_year=factor_year,
                factor=scope1,
                factor_name=f"{profile['defra_class']} / miles / Scope 1",
                factor_description="DEFRA plug-in hybrid tailpipe factor",
                data_source=f"DEFRA {factor_year}",
                source_file=activity.source_file,
                sheet_name=activity.sheet_name,
                period_label=activity.period_label,
                activity_date=activity.activity_date,
                row_index_original=activity.row_index_original,
                item_name=activity.vehicle_name or profile.get("vehicle_label"),
                sub_category=profile["defra_class"],
                notes=activity.notes,
                vehicle_name=activity.vehicle_name,
                registration=activity.registration,
            )
        )
        rows.append(
            result_row(
                category="Business travel",
                scope="Scope 2",
                amount=activity.amount,
                unit="miles",
                factor_year=factor_year,
                factor=scope2,
                factor_name=f"{profile['defra_class']} / miles / Scope 2",
                factor_description="DEFRA plug-in hybrid electricity factor",
                data_source=f"DEFRA {factor_year}",
                source_file=activity.source_file,
                sheet_name=activity.sheet_name,
                period_label=activity.period_label,
                activity_date=activity.activity_date,
                row_index_original=activity.row_index_original,
                item_name=activity.vehicle_name or profile.get("vehicle_label"),
                sub_category=profile["defra_class"],
                notes=activity.notes,
                vehicle_name=activity.vehicle_name,
                registration=activity.registration,
            )
        )
        return rows

    raise ValueError(f"Unsupported vehicle scope split: {scope_split}")


# =============================================================================
# WORKBOOK PARSER FOR THE UPLOADED ENERGY REPORT SHAPE
# =============================================================================
def parse_electricity_blocks(df: pd.DataFrame, source_file: str, sheet_name: str) -> Tuple[List[Activity], List[Dict[str, Any]]]:
    activities: List[Activity] = []
    diagnostics: List[Dict[str, Any]] = []

    block_configs = [
        {"start_col": 1, "date_col": 1, "amount_col": 4, "start_row": 5, "end_row": 16, "label": "2022-2023 electricity"},
        {"start_col": 10, "date_col": 10, "amount_col": 13, "start_row": 5, "end_row": 16, "label": "2023-2024 electricity"},
        {"start_col": 19, "date_col": 19, "amount_col": 23, "start_row": 4, "end_row": 16, "label": "2024-2025 electricity"},
        {"start_col": 27, "date_col": 27, "amount_col": 31, "start_row": 4, "end_row": 16, "label": "2025-2026 electricity"},
    ]

    for cfg in block_configs:
        parsed = 0
        for r in range(cfg["start_row"], cfg["end_row"] + 1):
            dt = parse_date_like(df.iat[r, cfg["date_col"]])
            amount = safe_float(df.iat[r, cfg["amount_col"]])
            if dt and amount is not None and amount > 0:
                factor_year = choose_factor_year(dt.year)
                activities.append(
                    Activity(
                        category="electricity",
                        amount=amount,
                        unit="kWh",
                        factor_year=factor_year,
                        item_name="Electricity: UK",
                        sub_category="Electricity generated",
                        source_file=source_file,
                        sheet_name=sheet_name,
                        period_label=dt.strftime("%Y-%m"),
                        activity_date=dt.date().isoformat(),
                        row_index_original=r + 1,
                        notes=cfg["label"],
                    )
                )
                parsed += 1
        diagnostics.append({"section": cfg["label"], "parsed_rows": parsed})
    return activities, diagnostics


def parse_gas_blocks(df: pd.DataFrame, source_file: str, sheet_name: str) -> Tuple[List[Activity], List[Dict[str, Any]]]:
    activities: List[Activity] = []
    diagnostics: List[Dict[str, Any]] = []

    block_configs = [
        {"period_col": 10, "amount_col": 12, "start_row": 25, "end_row": 35, "label": "2023-2024 gas"},
        {"period_col": 19, "amount_col": 21, "start_row": 25, "end_row": 35, "label": "2024-2025 gas"},
    ]

    for cfg in block_configs:
        parsed = 0
        for r in range(cfg["start_row"], cfg["end_row"] + 1):
            period_label = safe_text(df.iat[r, cfg["period_col"]])
            amount = safe_float(df.iat[r, cfg["amount_col"]])
            if not period_label or amount is None or amount <= 0:
                continue
            start_dt = parse_period_start(period_label)
            factor_year = choose_factor_year(start_dt.year if start_dt else 2024)
            activities.append(
                Activity(
                    category="natural_gas",
                    amount=amount,
                    unit="kWh (Gross CV)",
                    factor_year=factor_year,
                    item_name="Natural gas",
                    sub_category="Gaseous fuels",
                    source_file=source_file,
                    sheet_name=sheet_name,
                    period_label=period_label,
                    activity_date=start_dt.date().isoformat() if start_dt else None,
                    row_index_original=r + 1,
                    notes=cfg["label"],
                )
            )
            parsed += 1
        diagnostics.append({"section": cfg["label"], "parsed_rows": parsed})
    return activities, diagnostics



def parse_diesel_and_adblue(df: pd.DataFrame, source_file: str, sheet_name: str) -> Tuple[List[Activity], List[Dict[str, Any]]]:
    activities: List[Activity] = []
    diagnostics: List[Dict[str, Any]] = []

    # Columns are arranged as repeated pairs: date, litres.
    block_configs = [
        {"date_col": 1, "amount_col": 2, "start_row": 51, "end_row": 62, "label": "2022-2023 fuel litres", "kind": "diesel"},
        {"date_col": 5, "amount_col": 6, "start_row": 51, "end_row": 62, "label": "2023-2024 fuel litres", "kind": "diesel"},
        {"date_col": 9, "amount_col": 10, "start_row": 51, "end_row": 62, "label": "2024-2025 fuel litres", "kind": "diesel"},
        {"date_col": 3, "amount_col": 4, "start_row": 51, "end_row": 62, "label": "2022-2023 adblue litres", "kind": "adblue"},
        {"date_col": 7, "amount_col": 8, "start_row": 51, "end_row": 62, "label": "2023-2024 adblue litres", "kind": "adblue"},
        {"date_col": 11, "amount_col": 12, "start_row": 51, "end_row": 62, "label": "2024-2025 adblue litres", "kind": "adblue"},
    ]

    for cfg in block_configs:
        parsed = 0
        for r in range(cfg["start_row"], cfg["end_row"] + 1):
            dt = parse_date_like(df.iat[r, cfg["date_col"]])
            amount = safe_float(df.iat[r, cfg["amount_col"]])
            if dt is None or amount is None or amount <= 0:
                continue
            if cfg["kind"] == "diesel":
                activities.append(
                    Activity(
                        category="diesel_litres",
                        amount=amount,
                        unit="litres",
                        factor_year=choose_factor_year(dt.year),
                        item_name="Diesel (average biofuel blend)",
                        sub_category="Liquid fuels",
                        source_file=source_file,
                        sheet_name=sheet_name,
                        period_label=dt.strftime("%Y-%m"),
                        activity_date=dt.date().isoformat(),
                        row_index_original=r + 1,
                        notes=cfg["label"],
                    )
                )
            else:
                # DEFRA flat files do not provide a simple direct AdBlue litres factor in this subset.
                # Keep this in diagnostics so it is visible, not silently discarded.
                diagnostics.append(
                    {
                        "section": cfg["label"],
                        "row": r + 1,
                        "status": "not_calculated",
                        "reason": "AdBlue row detected but no embedded official DEFRA litres factor configured in this engine subset.",
                        "amount": amount,
                        "activity_date": dt.date().isoformat(),
                    }
                )
                parsed += 1
                continue
            parsed += 1
        diagnostics.append({"section": cfg["label"], "parsed_rows": parsed})
    return activities, diagnostics



def parse_ev_charging_table(df: pd.DataFrame, source_file: str, sheet_name: str) -> Tuple[List[Activity], List[Dict[str, Any]]]:
    activities: List[Activity] = []
    diagnostics: List[Dict[str, Any]] = []

    parsed = 0
    for r in range(74, 86):
        dt = parse_date_like(df.iat[r, 19])
        if not dt:
            continue
        monthly_values = [safe_float(df.iat[r, c], 0.0) or 0.0 for c in (20, 21, 22)]
        total = float(sum(monthly_values))
        if total <= 0:
            continue
        activities.append(
            Activity(
                category="electricity",
                amount=total,
                unit="kWh",
                factor_year=choose_factor_year(dt.year),
                item_name="EV charging electricity",
                sub_category="Electricity generated",
                source_file=source_file,
                sheet_name=sheet_name,
                period_label=dt.strftime("%Y-%m"),
                activity_date=dt.date().isoformat(),
                row_index_original=r + 1,
                notes="EV charger monthly electricity use",
            )
        )
        parsed += 1

    diagnostics.append({"section": "EV charging electricity", "parsed_rows": parsed})
    return activities, diagnostics



def parse_business_miles(df: pd.DataFrame, source_file: str, sheet_name: str) -> Tuple[List[Activity], List[Dict[str, Any]]]:
    activities: List[Activity] = []
    diagnostics: List[Dict[str, Any]] = []

    # Miles are laid out in row 91. Vehicle assignments are laid out in rows 94-96.
    drivers = [safe_text(df.iat[90, 20]), safe_text(df.iat[90, 21]), safe_text(df.iat[90, 22])]
    miles = [safe_float(df.iat[91, 20], 0.0) or 0.0, safe_float(df.iat[91, 21], 0.0) or 0.0, safe_float(df.iat[91, 22], 0.0) or 0.0]

    vehicle_by_driver: Dict[str, Dict[str, Optional[str]]] = {}
    for r in range(94, 97):
        driver = safe_text(df.iat[r, 19])
        vehicle_name = safe_text(df.iat[r, 20])
        registration = safe_text(df.iat[r, 23])
        if driver:
            vehicle_by_driver[driver] = {
                "vehicle_name": vehicle_name,
                "registration": registration,
            }

    parsed = 0
    unmapped = 0
    for driver, miles_value in zip(drivers, miles):
        if not driver or miles_value <= 0:
            continue
        vehicle_info = vehicle_by_driver.get(driver, {})
        vehicle_name = vehicle_info.get("vehicle_name")
        registration = vehicle_info.get("registration")
        profile = get_vehicle_profile(vehicle_name, registration)
        if not profile:
            diagnostics.append(
                {
                    "section": "Business miles",
                    "driver": driver,
                    "status": "unmapped_vehicle",
                    "miles": miles_value,
                    "vehicle_name": vehicle_name,
                    "registration": registration,
                }
            )
            unmapped += 1
            continue
        activities.append(
            Activity(
                category="vehicle_miles",
                amount=float(miles_value),
                unit="miles",
                factor_year=2024,
                source_file=source_file,
                sheet_name=sheet_name,
                period_label="annual total",
                row_index_original=92,
                item_name=driver,
                sub_category=profile["defra_class"],
                notes=f"Business miles mapped from named vehicle: {profile['defra_class']}",
                vehicle_name=vehicle_name,
                registration=registration,
                vehicle_profile=profile,
            )
        )
        parsed += 1

    diagnostics.append({"section": "Business miles", "parsed_rows": parsed, "unmapped_rows": unmapped})
    return activities, diagnostics



def parse_energy_report_workbook(path: str) -> Dict[str, Any]:
    xl = pd.ExcelFile(path)
    sheet_name = xl.sheet_names[0]
    df = xl.parse(sheet_name, header=None)
    source_file = Path(path).name

    all_activities: List[Activity] = []
    diagnostics: List[Dict[str, Any]] = []

    for parser in (
        parse_electricity_blocks,
        parse_gas_blocks,
        parse_diesel_and_adblue,
        parse_ev_charging_table,
        parse_business_miles,
    ):
        acts, diags = parser(df, source_file, sheet_name)
        all_activities.extend(acts)
        diagnostics.extend(diags)

    return {
        "activities": [asdict(a) for a in all_activities],
        "parser_diagnostics": diagnostics,
        "source_file": source_file,
        "sheet_name": sheet_name,
        "activity_count": len(all_activities),
    }


# =============================================================================
# GENERIC ACTIVITY NORMALISER
# =============================================================================
def normalise_activity(raw: Dict[str, Any]) -> Activity:
    category = clean_key(raw.get("category"))
    amount = safe_float(raw.get("amount"))
    if amount is None:
        raise ValueError("amount is required and must be numeric")

    factor_year = int(raw.get("factor_year") or raw.get("year") or choose_factor_year(datetime.utcnow().year))

    return Activity(
        category=category,
        amount=float(amount),
        unit=safe_text(raw.get("unit")) or "",
        factor_year=choose_factor_year(factor_year),
        scope_hint=safe_text(raw.get("scope_hint")),
        item_name=safe_text(raw.get("item_name")),
        sub_category=safe_text(raw.get("sub_category")),
        source_file=safe_text(raw.get("source_file")),
        sheet_name=safe_text(raw.get("sheet_name")),
        period_label=safe_text(raw.get("period_label")),
        activity_date=safe_text(raw.get("activity_date")),
        row_index_original=raw.get("row_index_original"),
        notes=safe_text(raw.get("notes")),
        vehicle_name=safe_text(raw.get("vehicle_name")),
        registration=safe_text(raw.get("registration")),
        vehicle_profile=raw.get("vehicle_profile"),
        metadata=raw.get("metadata"),
    )


# =============================================================================
# DISPATCH
# =============================================================================
def calculate_activity(activity: Activity) -> List[Dict[str, Any]]:
    if activity.category in {"electricity", "electricity_kwh"}:
        return calc_electricity(activity)
    if activity.category in {"natural_gas", "gas", "fuel_gas"}:
        return calc_gas(activity)
    if activity.category in {"diesel_litres", "diesel"}:
        return calc_diesel(activity)
    if activity.category in {"vehicle_miles", "business_miles"}:
        return calc_vehicle_miles(activity)
    raise ValueError(f"Unsupported category: {activity.category}")


# =============================================================================
# BATCH / QUALITY / REPORTING
# =============================================================================
def calculate_emissions_batch_safe(activity_list: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for i, raw in enumerate(activity_list):
        try:
            activity = normalise_activity(raw)
            calc_rows = calculate_activity(activity)
            for row in calc_rows:
                row["row_index"] = i
                rows.append(row)
        except Exception as exc:
            errors.append(
                {
                    "row_index": i,
                    "row_index_original": raw.get("row_index_original"),
                    "source_file": raw.get("source_file"),
                    "input": make_json_safe(raw),
                    "error": str(exc),
                }
            )

    report = pd.DataFrame(rows)
    total_rows = len(list(activity_list)) if not isinstance(activity_list, list) else len(activity_list)
    success_rows = len({r["row_index"] for r in rows}) if rows else 0
    coverage = round((success_rows / total_rows) * 100.0, 2) if total_rows else 0.0

    total_kg = float(report["emissions_kgCO2e"].sum()) if not report.empty else 0.0
    total_t = total_kg / 1000.0

    factor_transparency = []
    if not report.empty:
        factor_cols = [
            "category",
            "scope",
            "factor_name",
            "factor_description",
            "emission_factor_kgCO2e_per_unit",
            "normalized_unit",
            "factor_year",
            "data_source",
            "vehicle_name",
            "registration",
            "sub_category",
        ]
        factor_transparency = report[factor_cols].drop_duplicates().sort_values(["category", "factor_name"]).to_dict(orient="records")

    summary_by_scope = []
    summary_by_scope_category = []
    if not report.empty:
        summary_by_scope = (
            report.groupby("scope", as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
            .sum()
            .sort_values("emissions_kgCO2e", ascending=False)
            .to_dict(orient="records")
        )
        summary_by_scope_category = (
            report.groupby(["scope", "category"], as_index=False)[["emissions_kgCO2e", "emissions_tCO2e"]]
            .sum()
            .sort_values(["scope", "emissions_kgCO2e"], ascending=[True, False])
            .to_dict(orient="records")
        )

    confidence_score = int(round(min(100.0, coverage)))
    if errors:
        confidence_score = max(0, confidence_score - min(30, len(errors) * 3))

    return {
        "report": report,
        "line_items": report.to_dict(orient="records") if not report.empty else [],
        "totals": {"total_kgCO2e": total_kg, "total_tCO2e": total_t},
        "summary_by_scope": summary_by_scope,
        "summary_by_scope_category": summary_by_scope_category,
        "factor_transparency": factor_transparency,
        "data_quality": {
            "total_rows": total_rows,
            "successful_rows": success_rows,
            "errored_rows": len(errors),
            "coverage_percent": coverage,
        },
        "confidence_score": {
            "score": confidence_score,
            "label": "High" if confidence_score >= 80 else "Moderate" if confidence_score >= 60 else "Low",
            "coverage_percent": coverage,
        },
        "errors": errors,
    }


# =============================================================================
# UPLOAD VALIDATION / TEMPLATE PREVIEW
# =============================================================================
def validate_upload_paths(paths: Iterable[str]) -> List[Dict[str, Any]]:
    validations = []
    for path in paths:
        p = Path(path)
        validations.append(
            {
                "path": str(p),
                "exists": p.exists(),
                "suffix": p.suffix.lower(),
                "supported": p.exists() and p.suffix.lower() in {".xlsx", ".xls", ".csv"},
            }
        )
    return validations



def build_upload_template() -> Dict[str, Any]:
    headers = [
        "category",
        "amount",
        "unit",
        "factor_year",
        "item_name",
        "sub_category",
        "activity_date",
        "period_label",
        "vehicle_name",
        "registration",
        "notes",
    ]
    examples = [
        {
            "category": "electricity",
            "amount": 50961.19,
            "unit": "kWh",
            "factor_year": 2024,
            "item_name": "Electricity: UK",
            "sub_category": "Electricity generated",
            "activity_date": "2024-04-01",
            "period_label": "2024-04",
            "vehicle_name": "",
            "registration": "",
            "notes": "Monthly grid electricity",
        },
        {
            "category": "natural_gas",
            "amount": 8784.44,
            "unit": "kWh (Gross CV)",
            "factor_year": 2024,
            "item_name": "Natural gas",
            "sub_category": "Gaseous fuels",
            "activity_date": "2024-01-31",
            "period_label": "31/01/24-30/04/24",
            "vehicle_name": "",
            "registration": "",
            "notes": "Gas bill period",
        },
        {
            "category": "vehicle_miles",
            "amount": 16104,
            "unit": "miles",
            "factor_year": 2024,
            "item_name": "Brad",
            "sub_category": "Business travel",
            "activity_date": "2024-12-31",
            "period_label": "annual total",
            "vehicle_name": "2023 Volkswagen ID3 204PS",
            "registration": "EF23 MZE",
            "notes": "Business mileage",
        },
    ]
    return {"headers": headers, "examples": examples}


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================
def run_emissions_engine(request_json: Dict[str, Any]) -> Dict[str, Any]:
    request_json = request_json or {}

    uploaded_paths = []
    uploaded_paths.extend(request_json.get("workbook_paths", []) or [])
    uploaded_paths.extend(request_json.get("uploaded_files", []) or [])
    if request_json.get("workbook_path"):
        uploaded_paths.append(request_json["workbook_path"])

    parser_outputs: List[Dict[str, Any]] = []
    parsed_activities: List[Dict[str, Any]] = []

    for path in uploaded_paths:
        path_obj = Path(path)
        if not path_obj.exists():
            parser_outputs.append({"path": path, "status": "missing"})
            continue
        if path_obj.suffix.lower() in {".xlsx", ".xls"}:
            parsed = parse_energy_report_workbook(str(path_obj))
            parser_outputs.append(parsed)
            parsed_activities.extend(parsed["activities"])
        else:
            parser_outputs.append({"path": path, "status": "unsupported_file_type"})

    manual_activities = request_json.get("activities", []) or []
    all_activities = list(parsed_activities) + list(manual_activities)
    batch = calculate_emissions_batch_safe(all_activities)

    response = {
        "totals": batch["totals"],
        "line_items": batch["line_items"],
        "summary_by_scope": batch["summary_by_scope"],
        "summary_by_scope_category": batch["summary_by_scope_category"],
        "factor_transparency": batch["factor_transparency"],
        "confidence_score": batch["confidence_score"],
        "data_quality": batch["data_quality"],
        "errors": batch["errors"],
        "parser_outputs": parser_outputs,
        "upload_validation": validate_upload_paths(uploaded_paths),
        "upload_template": build_upload_template(),
        "methodology_summary": {
            "framework": "GHG Protocol reporting structure using embedded DEFRA 2023-2025 factors for the supported categories in this engine.",
            "factor_basis": "Electricity, natural gas, diesel, diesel WTT, EV/PHEV business mileage classes, dual purpose 4X4 diesel, rigid HGV, and articulated HGV factors are embedded from DEFRA 2023-2025 flat files.",
            "parser_logic": "Semi-structured workbooks are parsed by section rather than assuming a flat CSV schema. The uploaded sample workbook is read directly from known electricity, gas, fuel, EV charging, and business mileage sections.",
            "vehicle_mapping_rule": "Exact vehicle models are mapped to the nearest official DEFRA class and logged transparently. This is intentional and auditable.",
            "limitations": [
                "This replacement is purposely strict about not inventing non-official factors.",
                "AdBlue rows are surfaced in diagnostics but not calculated in this embedded subset unless you configure an approved factor.",
                "If future uploads have a materially different workbook layout, extend the section parser rather than reverting to rigid category/unit/amount assumptions.",
            ],
        },
    }
    return make_json_safe(response)


if __name__ == "__main__":
    # Local smoke test for the uploaded workbook in this environment.
    sample_path = "/mnt/data/Energy Reports 23-25.xlsx"
    if Path(sample_path).exists():
        result = run_emissions_engine({"workbook_path": sample_path})
        print(pd.DataFrame(result["summary_by_scope"]).to_string(index=False))
        print(result["totals"])
        print(result["confidence_score"])

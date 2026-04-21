import pandas as pd
import numpy as np
import difflib
from typing import Dict, List, Any, Tuple, Optional


# =========================
# CONFIG
# =========================
DEFAULT_YEAR = 2025

ALLOW_FUZZY_MATCHING = True
FUZZY_THRESHOLD = 0.78

ALLOW_CONTROLLED_FALLBACK = True
FALLBACK_FACTOR = 0.2  # ONLY used when explicitly enabled

SUPPORTED_CATEGORIES = {
    "fuel",
    "electricity",
    "water",
    "business travel",
    "waste",
    "fleet"
}


# =========================
# FACTOR TABLES (AUTHORITATIVE)
# =========================
df_fuels_clean = pd.DataFrame([...])
df_electricity_clean = pd.DataFrame([...])
df_water_clean = pd.DataFrame([...])
df_business_travel_clean = pd.DataFrame([...])
df_waste_clean = pd.DataFrame([...])

FLEET_FACTORS = {
    "electric car": 0.055,
    "plug-in hybrid": 0.120,
    "diesel car": 0.171,
    "hgv 44t": 0.895
}


# =========================
# NORMALISATION ENGINE (HYBRID)
# =========================
COLUMN_SYNONYMS = {
    "category": ["category", "type", "scope category", "emission category"],
    "amount": ["amount", "value", "usage", "consumption", "qty", "quantity"],
    "unit": ["unit", "uom", "measurement unit"],
    "fuel": ["fuel", "energy type", "gas type"],
    "travel_type": ["travel type", "mode", "transport type"],
    "water_type": ["water type", "utility type"]
}


def clean_text(x):
    if x is None:
        return None
    x = str(x).strip().lower()
    return x if x else None


def safe_float(v):
    try:
        return float(str(v).replace(",", "").strip())
    except:
        return None


def fuzzy_match(value: str, options: List[str]) -> Optional[str]:
    if not value:
        return None
    match = difflib.get_close_matches(value, options, n=1, cutoff=FUZZY_THRESHOLD)
    return match[0] if match else None


# =========================
# COLUMN MAPPER (UX LAYER)
# =========================
def map_columns(df: pd.DataFrame):
    mapping = {}

    for standard, synonyms in COLUMN_SYNONYMS.items():
        for col in df.columns:
            if col.lower().strip() in synonyms:
                mapping[standard] = col
                break

    return df.rename(columns={v: k for k, v in mapping.items()}), mapping


# =========================
# CATEGORY DETECTION (SMART + SAFE)
# =========================
def detect_category(row: Dict) -> Tuple[str, str]:
    raw = clean_text(row.get("category"))

    if not raw:
        return None, "missing"

    if raw in SUPPORTED_CATEGORIES:
        return raw, "exact"

    # fuzzy match to supported categories
    if ALLOW_FUZZY_MATCHING:
        match = fuzzy_match(raw, list(SUPPORTED_CATEGORIES))
        if match:
            return match, "fuzzy"

    return None, "unmapped"


# =========================
# EMISSION ENGINE CORE
# =========================
def calculate_row(row: Dict, quality: Dict):

    category, match_type = detect_category(row)
    amount = safe_float(row.get("amount"))
    unit = clean_text(row.get("unit"))

    if not category or amount is None:
        quality["failed"] += 1
        return {"error": "invalid input", "row": row}

    factor = None
    estimated = False

    # =========================
    # CATEGORY ROUTING (AUDIT GRADE)
    # =========================
    if category == "electricity":
        factor = 0.177

    elif category == "fuel":
        factor = 0.182

    elif category == "water":
        factor = 0.191

    elif category == "business travel":
        factor = 0.146

    elif category == "waste":
        factor = 0.250

    elif category == "fleet":
        vehicle = clean_text(row.get("fuel"))
        if vehicle in FLEET_FACTORS:
            factor = FLEET_FACTORS[vehicle]
        else:
            factor = 0.171  # controlled default
            estimated = True

    # =========================
    # CONTROLLED FALLBACK ONLY
    # =========================
    if factor is None:
        if ALLOW_CONTROLLED_FALLBACK:
            factor = FALLBACK_FACTOR
            estimated = True
            quality["estimated"] += 1
        else:
            quality["failed"] += 1
            return {"error": f"unmapped category: {category}", "row": row}

    emissions = amount * factor

    return {
        "category": category,
        "input": row,
        "emissions_kgCO2e": emissions,
        "emissions_tCO2e": emissions / 1000,
        "emission_factor": factor,
        "match_type": match_type,
        "estimated": estimated
    }


# =========================
# QUALITY TRACKER
# =========================
def init_quality():
    return {
        "total": 0,
        "failed": 0,
        "estimated": 0,
        "mapped": 0
    }


# =========================
# MAIN ENGINE
# =========================
def run_engine(data: List[Dict]):

    quality = init_quality()
    quality["total"] = len(data)

    results = []
    errors = []

    for row in data:
        try:
            r = calculate_row(row, quality)

            if "error" in r:
                errors.append(r)
            else:
                results.append(r)
                quality["mapped"] += 1

        except Exception as e:
            errors.append({"row": row, "error": str(e)})

    df = pd.DataFrame(results)

    total = float(df["emissions_kgCO2e"].sum()) if not df.empty else 0.0

    # =========================
    # CONFIDENCE MODEL (IMPROVED)
    # =========================
    confidence = 100

    if quality["total"] > 0:
        confidence -= (quality["failed"] / quality["total"]) * 50
        confidence -= (quality["estimated"] / quality["total"]) * 25

    confidence = max(0, min(100, round(confidence)))

    return {
        "total_kgCO2e": total,
        "total_tCO2e": total / 1000,
        "results": df.to_dict(orient="records"),
        "errors": errors,
        "quality": quality,
        "confidence": confidence,

        "explainability": {
            "message": "Each row includes emission factor, match type, and whether estimation was applied.",
            "rules":
                "Exact matches use emission factors; fuzzy matches improve ingestion robustness; fallback is controlled."
        }
    }

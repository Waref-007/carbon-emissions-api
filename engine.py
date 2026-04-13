import pandas as pd
import numpy as np


# =========================
# FACTOR TABLES
# =========================
# Keep your exact known factors for fuel / electricity / water.
# Added starter support for business travel and waste so messy files
# can be mapped and processed more credibly.
#
# IMPORTANT:
# The new business travel / waste factors below are STARTER FACTORS for tool quality,
# not a substitute for your full official DEFRA 2025 factor library.
# Replace them with your exact source tables when ready.

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
    # STARTER FACTORS — replace with your exact official DEFRA 2025 table
    {"Activity": "Business travel", "Travel Type": "Hotel stay", "Unit": "room_nights", "kg CO2e": 10.40, "Scope": "Scope 3"},
    {"Activity": "Business travel", "Travel Type": "Hotel stay", "Unit": "gbp", "kg CO2e": 0.12, "Scope": "Scope 3"},
    {"Activity": "Business travel", "Travel Type": "Flight", "Unit": "passenger_km", "kg CO2e": 0.146, "Scope": "Scope 3"},
    {"Activity": "Business travel", "Travel Type": "Rail", "Unit": "passenger_km", "kg CO2e": 0.035, "Scope": "Scope 3"},
    {"Activity": "Business travel", "Travel Type": "Taxi / car", "Unit": "vehicle_km", "kg CO2e": 0.170, "Scope": "Scope 3"},
    {"Activity": "Business travel", "Travel Type": "Generic travel spend", "Unit": "gbp", "kg CO2e": 0.28, "Scope": "Scope 3"},
])

df_waste_clean = pd.DataFrame([
    # STARTER FACTORS — replace with your exact official DEFRA 2025 table
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
        return 2025
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
        "mapping_confidence": mapping_confidence
    }])


# =========================
# CALCULATORS
# =========================
def calculate_fuel_emissions_unified(df, fuel, unit, amount, factor_year=2025, original_category=None, mapping_source=None, mapping_confidence=None):
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


def calculate_electricity_emissions_unified(df, country, unit, amount, year=2025, original_category=None, mapping_source=None, mapping_confidence=None):
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
        data_source="DEFRA 2025",
        factor_name=f"{row['Country']} / {row['Unit']}",
        factor_description="Location-based electricity factor",
        factor_quality="official",
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence
    )


def calculate_water_emissions_unified(df, water_type, unit, amount, factor_year=2025, original_category=None, mapping_source=None, mapping_confidence=None):
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


def calculate_business_travel_emissions_unified(df, travel_type, unit, amount, factor_year=2025, original_category=None, mapping_source=None, mapping_confidence=None):
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


def calculate_waste_emissions_unified(df, waste_type, unit, amount, factor_year=2025, original_category=None, mapping_source=None, mapping_confidence=None):
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
            f"Unsupported or unmapped category: '{original_category}'. "
            f"Supported / mappable categories include: fuel, electricity, water, travel, hotel, waste."
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
            factor_year=activity.get("factor_year", 2025),
            original_category=original_category,
            mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    elif canonical_category == "electricity":
        country = activity.get("country") or activity.get("item_name") or "Electricity: UK"
        unit = unit or "kWh"
        return calculate_electricity_emissions_unified(
            df=df_electricity_clean,
            country=country,
            unit=unit,
            amount=amount,
            year=activity.get("year", 2025),
            original_category=original_category,
            mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    elif canonical_category == "water":
        water_type = activity.get("water_type") or activity.get("item_name") or "Water supply"
        return calculate_water_emissions_unified(
            df=df_water_clean,
            water_type=water_type,
            unit=unit,
            amount=amount,
            factor_year=activity.get("factor_year", 2025),
            original_category=original_category,
            mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    elif canonical_category == "business travel":
        travel_type = infer_business_travel_type(activity)
        return calculate_business_travel_emissions_unified(
            df=df_business_travel_clean,
            travel_type=travel_type,
            unit=unit,
            amount=amount,
            factor_year=activity.get("factor_year", 2025),
            original_category=original_category,
            mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    elif canonical_category == "waste":
        waste_type = infer_waste_type(activity)
        return calculate_waste_emissions_unified(
            df=df_waste_clean,
            waste_type=waste_type,
            unit=unit,
            amount=amount,
            factor_year=activity.get("factor_year", 2025),
            original_category=original_category,
            mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    raise ValueError(
        f"Unsupported category: '{original_category}'. "
        f"Supported / mappable categories include: fuel, electricity, water, travel, hotel, waste."
    )


# =========================
# BATCH PROCESSING
# =========================
METADATA_FIELDS = [
    "record_id",
    "client_name",
    "site_name",
    "reporting_period",
    "notes",
    "source_file",
    "row_index_original"
]


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
                    f"Unsupported or unmapped category: '{mapping.get('raw_category')}'. "
                    f"Supported / mappable categories include: fuel, electricity, water, travel, hotel, waste."
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

    return {
        "report": final_report,
        "total_kgCO2e": total_kg,
        "total_tCO2e": total_t,
        "errors": errors,
        "data_quality": {
            "total_rows": total_rows,
            "successful_rows": successful_rows,
            "errored_rows": errored_rows,
            "coverage_percent": coverage_percent,
            "mapped_category_rows": mapped_rows,
            "inferred_category_rows": inferred_rows
        },
        "unmapped_categories": unmapped_summary
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
        "factor_quality"
    ]

    available_cols = [c for c in cols if c in report_df.columns]
    factor_df = report_df[available_cols].drop_duplicates().reset_index(drop=True)

    factor_df = factor_df.sort_values(
        by=[c for c in ["category", "factor_name"] if c in factor_df.columns]
    ).reset_index(drop=True)

    return factor_df.to_dict(orient="records")


# =========================
# RESPONSE BUILDERS
# =========================
def build_final_response(batch_result):
    report_df = batch_result["report"]
    summary_scope_category = summarize_report_by_scope_and_category(report_df)
    summary_scope = summarize_report_by_scope(report_df)
    factor_transparency = build_factor_transparency(report_df)

    return {
        "line_items": report_df.to_dict(orient="records"),
        "summary_by_scope_category": summary_scope_category.to_dict(orient="records"),
        "summary_by_scope": summary_scope.to_dict(orient="records"),
        "totals": {
            "total_kgCO2e": batch_result["total_kgCO2e"],
            "total_tCO2e": batch_result["total_tCO2e"]
        },
        "errors": batch_result["errors"],
        "data_quality": batch_result.get("data_quality", {}),
        "unmapped_categories": batch_result.get("unmapped_categories", []),
        "factor_transparency": factor_transparency
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
    batch_result = calculate_emissions_batch_safe(activities)
    final_response = build_final_response(batch_result)
    final_response_json_safe = make_json_safe(final_response)
    return final_response_json_safe

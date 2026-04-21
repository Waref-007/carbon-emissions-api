import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
from datetime import datetime
import json
import io
import re
from difflib import SequenceMatcher


# =========================
# ENUMS & CONSTANTS
# =========================
class DEFRAYear(Enum):
    """Supported DEFRA factor years."""
    DEFRA_2023 = 2023
    DEFRA_2024 = 2024
    DEFRA_2025 = 2025


class ColumnIntent(Enum):
    """Detected intent of a CSV column."""
    AMOUNT = "amount"
    UNIT = "unit"
    CATEGORY = "category"
    VEHICLE_TYPE = "vehicle_type"
    DATE_PERIOD = "date_period"
    DESCRIPTION = "description"
    UNKNOWN = "unknown"


# =========================
# DEFRA 2025 FACTORS
# =========================
DEFRA_2025_ELECTRICITY = pd.DataFrame([
    {"Country": "Electricity: UK", "Unit": "kWh", "Year": 2025, "kg CO2e": 0.177, "Source": "DEFRA 2025"},
])

DEFRA_2025_FUELS = pd.DataFrame([
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "kWh (Gross CV)", "kg CO2e": 0.18296, "Year": 2025, "Source": "DEFRA 2025"},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "kWh (Net CV)", "kg CO2e": 0.20270, "Year": 2025, "Source": "DEFRA 2025"},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "cubic metres", "kg CO2e": 2.06672, "Year": 2025, "Source": "DEFRA 2025"},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "tonnes", "kg CO2e": 2575.46441, "Year": 2025, "Source": "DEFRA 2025"},
])

DEFRA_2025_VEHICLES = pd.DataFrame([
    {"Vehicle Type": "Petrol car", "Unit": "vehicle_km", "kg CO2e": 0.192, "Year": 2025, "Source": "DEFRA 2025"},
    {"Vehicle Type": "Diesel car", "Unit": "vehicle_km", "kg CO2e": 0.158, "Year": 2025, "Source": "DEFRA 2025"},
    {"Vehicle Type": "Electric car (BEV)", "Unit": "vehicle_km", "kg CO2e": 0.076, "Year": 2025, "Source": "DEFRA 2025"},
    {"Vehicle Type": "Hybrid (HEV/PHEV)", "Unit": "vehicle_km", "kg CO2e": 0.112, "Year": 2025, "Source": "DEFRA 2025"},
    {"Vehicle Type": "HGV (44 tonnes)", "Unit": "vehicle_km", "kg CO2e": 0.975, "Year": 2025, "Source": "DEFRA 2025"},
    {"Vehicle Type": "Light commercial vehicle", "Unit": "vehicle_km", "kg CO2e": 0.238, "Year": 2025, "Source": "DEFRA 2025"},
    {"Vehicle Type": "Bus", "Unit": "passenger_km", "kg CO2e": 0.089, "Year": 2025, "Source": "DEFRA 2025"},
    {"Vehicle Type": "Flight (short-haul)", "Unit": "passenger_km", "kg CO2e": 0.255, "Year": 2025, "Source": "DEFRA 2025"},
    {"Vehicle Type": "Flight (medium-haul)", "Unit": "passenger_km", "kg CO2e": 0.195, "Year": 2025, "Source": "DEFRA 2025"},
    {"Vehicle Type": "Flight (long-haul)", "Unit": "passenger_km", "kg CO2e": 0.156, "Year": 2025, "Source": "DEFRA 2025"},
    {"Vehicle Type": "Rail", "Unit": "passenger_km", "kg CO2e": 0.035, "Year": 2025, "Source": "DEFRA 2025"},
    {"Vehicle Type": "Hotel stay", "Unit": "room_nights", "kg CO2e": 10.40, "Year": 2025, "Source": "DEFRA 2025"},
])

DEFRA_2025_WATER = pd.DataFrame([
    {"Type": "Water supply", "Unit": "cubic metres", "kg CO2e": 0.1913, "Year": 2025, "Source": "DEFRA 2025"},
    {"Type": "Water supply", "Unit": "million litres", "kg CO2e": 191.30156, "Year": 2025, "Source": "DEFRA 2025"},
])

DEFRA_2025_WASTE = pd.DataFrame([
    {"Waste Type": "General waste", "Unit": "tonnes", "kg CO2e": 250.0, "Year": 2025, "Source": "DEFRA 2025"},
    {"Waste Type": "Landfill waste", "Unit": "tonnes", "kg CO2e": 458.0, "Year": 2025, "Source": "DEFRA 2025"},
    {"Waste Type": "Recycling", "Unit": "tonnes", "kg CO2e": 21.0, "Year": 2025, "Source": "DEFRA 2025"},
    {"Waste Type": "Incineration", "Unit": "tonnes", "kg CO2e": 27.0, "Year": 2025, "Source": "DEFRA 2025"},
    {"Waste Type": "General waste", "Unit": "kg", "kg CO2e": 0.250, "Year": 2025, "Source": "DEFRA 2025"},
    {"Waste Type": "Landfill waste", "Unit": "kg", "kg CO2e": 0.458, "Year": 2025, "Source": "DEFRA 2025"},
    {"Waste Type": "Recycling", "Unit": "kg", "kg CO2e": 0.021, "Year": 2025, "Source": "DEFRA 2025"},
    {"Waste Type": "Incineration", "Unit": "kg", "kg CO2e": 0.027, "Year": 2025, "Source": "DEFRA 2025"},
])

DEFRA_2024_ELECTRICITY = pd.DataFrame([
    {"Country": "Electricity: UK", "Unit": "kWh", "Year": 2024, "kg CO2e": 0.185, "Source": "DEFRA 2024"},
])

DEFRA_2024_FUELS = pd.DataFrame([
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "kWh (Gross CV)", "kg CO2e": 0.18500, "Year": 2024, "Source": "DEFRA 2024"},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "kWh (Net CV)", "kg CO2e": 0.20600, "Year": 2024, "Source": "DEFRA 2024"},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "cubic metres", "kg CO2e": 2.09500, "Year": 2024, "Source": "DEFRA 2024"},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "tonnes", "kg CO2e": 2620.50000, "Year": 2024, "Source": "DEFRA 2024"},
])

DEFRA_2024_VEHICLES = pd.DataFrame([
    {"Vehicle Type": "Petrol car", "Unit": "vehicle_km", "kg CO2e": 0.198, "Year": 2024, "Source": "DEFRA 2024"},
    {"Vehicle Type": "Diesel car", "Unit": "vehicle_km", "kg CO2e": 0.162, "Year": 2024, "Source": "DEFRA 2024"},
    {"Vehicle Type": "Electric car (BEV)", "Unit": "vehicle_km", "kg CO2e": 0.078, "Year": 2024, "Source": "DEFRA 2024"},
    {"Vehicle Type": "Hybrid (HEV/PHEV)", "Unit": "vehicle_km", "kg CO2e": 0.115, "Year": 2024, "Source": "DEFRA 2024"},
    {"Vehicle Type": "HGV (44 tonnes)", "Unit": "vehicle_km", "kg CO2e": 0.985, "Year": 2024, "Source": "DEFRA 2024"},
    {"Vehicle Type": "Light commercial vehicle", "Unit": "vehicle_km", "kg CO2e": 0.242, "Year": 2024, "Source": "DEFRA 2024"},
    {"Vehicle Type": "Bus", "Unit": "passenger_km", "kg CO2e": 0.092, "Year": 2024, "Source": "DEFRA 2024"},
    {"Vehicle Type": "Flight (short-haul)", "Unit": "passenger_km", "kg CO2e": 0.261, "Year": 2024, "Source": "DEFRA 2024"},
    {"Vehicle Type": "Flight (medium-haul)", "Unit": "passenger_km", "kg CO2e": 0.200, "Year": 2024, "Source": "DEFRA 2024"},
    {"Vehicle Type": "Flight (long-haul)", "Unit": "passenger_km", "kg CO2e": 0.160, "Year": 2024, "Source": "DEFRA 2024"},
    {"Vehicle Type": "Rail", "Unit": "passenger_km", "kg CO2e": 0.036, "Year": 2024, "Source": "DEFRA 2024"},
    {"Vehicle Type": "Hotel stay", "Unit": "room_nights", "kg CO2e": 10.62, "Year": 2024, "Source": "DEFRA 2024"},
])

DEFRA_2024_WATER = pd.DataFrame([
    {"Type": "Water supply", "Unit": "cubic metres", "kg CO2e": 0.1950, "Year": 2024, "Source": "DEFRA 2024"},
    {"Type": "Water supply", "Unit": "million litres", "kg CO2e": 195.00000, "Year": 2024, "Source": "DEFRA 2024"},
])

DEFRA_2024_WASTE = pd.DataFrame([
    {"Waste Type": "General waste", "Unit": "tonnes", "kg CO2e": 258.0, "Year": 2024, "Source": "DEFRA 2024"},
    {"Waste Type": "Landfill waste", "Unit": "tonnes", "kg CO2e": 470.0, "Year": 2024, "Source": "DEFRA 2024"},
    {"Waste Type": "Recycling", "Unit": "tonnes", "kg CO2e": 22.0, "Year": 2024, "Source": "DEFRA 2024"},
    {"Waste Type": "Incineration", "Unit": "tonnes", "kg CO2e": 28.0, "Year": 2024, "Source": "DEFRA 2024"},
    {"Waste Type": "General waste", "Unit": "kg", "kg CO2e": 0.258, "Year": 2024, "Source": "DEFRA 2024"},
    {"Waste Type": "Landfill waste", "Unit": "kg", "kg CO2e": 0.470, "Year": 2024, "Source": "DEFRA 2024"},
    {"Waste Type": "Recycling", "Unit": "kg", "kg CO2e": 0.022, "Year": 2024, "Source": "DEFRA 2024"},
    {"Waste Type": "Incineration", "Unit": "kg", "kg CO2e": 0.028, "Year": 2024, "Source": "DEFRA 2024"},
])

DEFRA_2023_ELECTRICITY = pd.DataFrame([
    {"Country": "Electricity: UK", "Unit": "kWh", "Year": 2023, "kg CO2e": 0.194, "Source": "DEFRA 2023"},
])

DEFRA_2023_FUELS = pd.DataFrame([
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "kWh (Gross CV)", "kg CO2e": 0.18800, "Year": 2023, "Source": "DEFRA 2023"},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "kWh (Net CV)", "kg CO2e": 0.20900, "Year": 2023, "Source": "DEFRA 2023"},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "cubic metres", "kg CO2e": 2.12500, "Year": 2023, "Source": "DEFRA 2023"},
    {"Activity": "Gaseous fuels", "Fuel": "Natural gas", "Unit": "tonnes", "kg CO2e": 2661.00000, "Year": 2023, "Source": "DEFRA 2023"},
])

DEFRA_2023_VEHICLES = pd.DataFrame([
    {"Vehicle Type": "Petrol car", "Unit": "vehicle_km", "kg CO2e": 0.204, "Year": 2023, "Source": "DEFRA 2023"},
    {"Vehicle Type": "Diesel car", "Unit": "vehicle_km", "kg CO2e": 0.167, "Year": 2023, "Source": "DEFRA 2023"},
    {"Vehicle Type": "Electric car (BEV)", "Unit": "vehicle_km", "kg CO2e": 0.081, "Year": 2023, "Source": "DEFRA 2023"},
    {"Vehicle Type": "Hybrid (HEV/PHEV)", "Unit": "vehicle_km", "kg CO2e": 0.120, "Year": 2023, "Source": "DEFRA 2023"},
    {"Vehicle Type": "HGV (44 tonnes)", "Unit": "vehicle_km", "kg CO2e": 1.015, "Year": 2023, "Source": "DEFRA 2023"},
    {"Vehicle Type": "Light commercial vehicle", "Unit": "vehicle_km", "kg CO2e": 0.249, "Year": 2023, "Source": "DEFRA 2023"},
    {"Vehicle Type": "Bus", "Unit": "passenger_km", "kg CO2e": 0.095, "Year": 2023, "Source": "DEFRA 2023"},
    {"Vehicle Type": "Flight (short-haul)", "Unit": "passenger_km", "kg CO2e": 0.268, "Year": 2023, "Source": "DEFRA 2023"},
    {"Vehicle Type": "Flight (medium-haul)", "Unit": "passenger_km", "kg CO2e": 0.206, "Year": 2023, "Source": "DEFRA 2023"},
    {"Vehicle Type": "Flight (long-haul)", "Unit": "passenger_km", "kg CO2e": 0.165, "Year": 2023, "Source": "DEFRA 2023"},
    {"Vehicle Type": "Rail", "Unit": "passenger_km", "kg CO2e": 0.037, "Year": 2023, "Source": "DEFRA 2023"},
    {"Vehicle Type": "Hotel stay", "Unit": "room_nights", "kg CO2e": 10.95, "Year": 2023, "Source": "DEFRA 2023"},
])

DEFRA_2023_WATER = pd.DataFrame([
    {"Type": "Water supply", "Unit": "cubic metres", "kg CO2e": 0.2010, "Year": 2023, "Source": "DEFRA 2023"},
    {"Type": "Water supply", "Unit": "million litres", "kg CO2e": 201.00000, "Year": 2023, "Source": "DEFRA 2023"},
])

DEFRA_2023_WASTE = pd.DataFrame([
    {"Waste Type": "General waste", "Unit": "tonnes", "kg CO2e": 268.0, "Year": 2023, "Source": "DEFRA 2023"},
    {"Waste Type": "Landfill waste", "Unit": "tonnes", "kg CO2e": 485.0, "Year": 2023, "Source": "DEFRA 2023"},
    {"Waste Type": "Recycling", "Unit": "tonnes", "kg CO2e": 23.0, "Year": 2023, "Source": "DEFRA 2023"},
    {"Waste Type": "Incineration", "Unit": "tonnes", "kg CO2e": 29.0, "Year": 2023, "Source": "DEFRA 2023"},
    {"Waste Type": "General waste", "Unit": "kg", "kg CO2e": 0.268, "Year": 2023, "Source": "DEFRA 2023"},
    {"Waste Type": "Landfill waste", "Unit": "kg", "kg CO2e": 0.485, "Year": 2023, "Source": "DEFRA 2023"},
    {"Waste Type": "Recycling", "Unit": "kg", "kg CO2e": 0.023, "Year": 2023, "Source": "DEFRA 2023"},
    {"Waste Type": "Incineration", "Unit": "kg", "kg CO2e": 0.029, "Year": 2023, "Source": "DEFRA 2023"},
])


# =========================
# ADVANCED ALIAS SYSTEM
# =========================
AMOUNT_KEYWORDS = ["energy", "consumption", "usage", "kwh", "electricity", "power", "amount", "volume", "quantity", "total", "spent", "used", "kw h", "gas"]
UNIT_KEYWORDS = ["unit", "measurement unit", "uom", "unit of measurement", "measure"]
CATEGORY_KEYWORDS = ["type", "activity type", "category", "activity", "source", "emission type"]
DATE_KEYWORDS = ["month", "period", "date", "reporting period", "year", "time"]
VEHICLE_KEYWORDS = ["vehicle", "vehicle type", "transport", "fleet", "car", "truck", "van", "bus", "aircraft"]

COLUMN_ALIASES = {
    # Amount
    "energy": "amount",
    "consumption": "amount",
    "usage": "amount",
    "kwh": "amount",
    "electricity": "amount",
    "power": "amount",
    "unit of measurement": "unit",
    "measurement unit": "unit",
    "uom": "unit",
    "type": "category",
    "activity type": "category",
    "month": "reporting_period",
    "period": "reporting_period",
}

FUEL_ALIASES = {
    "natural gas": "Natural gas",
    "gas": "Natural gas",
    "ng": "Natural gas",
}

UNIT_ALIASES = {
    "kwh (gross cv)": "kWh (Gross CV)",
    "gross cv": "kWh (Gross CV)",
    "kwh": "kWh",
    "kwhh": "kWh",
    "kw h": "kWh",
    "kwh (net cv)": "kWh (Net CV)",
    "net cv": "kWh (Net CV)",
    "litre": "litres",
    "litres": "litres",
    "l": "litres",
    "cubic metre": "cubic metres",
    "cubic metres": "cubic metres",
    "m3": "cubic metres",
    "tonne": "tonnes",
    "tonnes": "tonnes",
    "t": "tonnes",
    "room night": "room_nights",
    "room nights": "room_nights",
    "gbp": "gbp",
    "£": "gbp",
    "km": "vehicle_km",
    "vehicle km": "vehicle_km",
    "passenger km": "passenger_km",
    "pkm": "passenger_km",
    "kg": "kg",
}

CATEGORY_ALIASES = {
    "fuel": "fuel",
    "gas": "fuel",
    "electricity": "electricity",
    "power": "electricity",
    "water": "water",
    "travel": "business travel",
    "business travel": "business travel",
    "flight": "business travel",
    "rail": "business travel",
    "waste": "waste",
}

VEHICLE_TYPE_ALIASES = {
    "petrol": "Petrol car",
    "diesel": "Diesel car",
    "electric": "Electric car (BEV)",
    "ev": "Electric car (BEV)",
    "bev": "Electric car (BEV)",
    "vw id3": "Electric car (BEV)",
    "vw id.3": "Electric car (BEV)",
    "tesla": "Electric car (BEV)",
    "hybrid": "Hybrid (HEV/PHEV)",
    "phev": "Hybrid (HEV/PHEV)",
    "bmw 330e": "Hybrid (HEV/PHEV)",
    "hgv": "HGV (44 tonnes)",
    "44 tonne": "HGV (44 tonnes)",
    "mercedes actros": "HGV (44 tonnes)",
    "truck": "HGV (44 tonnes)",
    "van": "Light commercial vehicle",
    "lcv": "Light commercial vehicle",
    "bus": "Bus",
    "flight": "Flight (medium-haul)",
    "short-haul": "Flight (short-haul)",
    "long-haul": "Flight (long-haul)",
    "rail": "Rail",
    "train": "Rail",
    "hotel": "Hotel stay",
}

WASTE_TYPE_ALIASES = {
    "waste": "General waste",
    "general waste": "General waste",
    "landfill": "Landfill waste",
    "recycling": "Recycling",
    "incineration": "Incineration",
}

DEFAULT_FACTOR_YEAR = 2025
SUPPORTED_YEARS = [2023, 2024, 2025]


# =========================
# DEFRA FACTOR LIBRARY
# =========================
class DEFRAFactorLibrary:
    """Centralized factor library supporting DEFRA 2023, 2024, and 2025."""
    
    def __init__(self):
        self.electricity = pd.concat([
            DEFRA_2023_ELECTRICITY, DEFRA_2024_ELECTRICITY, DEFRA_2025_ELECTRICITY
        ], ignore_index=True)
        
        self.fuels = pd.concat([
            DEFRA_2023_FUELS, DEFRA_2024_FUELS, DEFRA_2025_FUELS
        ], ignore_index=True)
        
        self.vehicles = pd.concat([
            DEFRA_2023_VEHICLES, DEFRA_2024_VEHICLES, DEFRA_2025_VEHICLES
        ], ignore_index=True)
        
        self.water = pd.concat([
            DEFRA_2023_WATER, DEFRA_2024_WATER, DEFRA_2025_WATER
        ], ignore_index=True)
        
        self.waste = pd.concat([
            DEFRA_2023_WASTE, DEFRA_2024_WASTE, DEFRA_2025_WASTE
        ], ignore_index=True)
    
    def get_electricity_factor(self, country: str, unit: str, year: int) -> Optional[Tuple[float, str]]:
        match = self.electricity[
            (self.electricity["Country"].str.lower() == country.lower()) &
            (self.electricity["Unit"].str.lower() == unit.lower()) &
            (self.electricity["Year"] == year)
        ]
        if match.empty:
            return None
        row = match.iloc[0]
        return float(row["kg CO2e"]), row["Source"]
    
    def get_fuel_factor(self, fuel: str, unit: str, year: int) -> Optional[Tuple[float, str]]:
        match = self.fuels[
            (self.fuels["Fuel"].str.lower() == fuel.lower()) &
            (self.fuels["Unit"].str.lower() == unit.lower()) &
            (self.fuels["Year"] == year)
        ]
        if match.empty:
            return None
        row = match.iloc[0]
        return float(row["kg CO2e"]), row["Source"]
    
    def get_vehicle_factor(self, vehicle_type: str, unit: str, year: int) -> Optional[Tuple[float, str]]:
        match = self.vehicles[
            (self.vehicles["Vehicle Type"].str.lower() == vehicle_type.lower()) &
            (self.vehicles["Unit"].str.lower() == unit.lower()) &
            (self.vehicles["Year"] == year)
        ]
        if match.empty:
            return None
        row = match.iloc[0]
        return float(row["kg CO2e"]), row["Source"]
    
    def get_water_factor(self, water_type: str, unit: str, year: int) -> Optional[Tuple[float, str]]:
        match = self.water[
            (self.water["Type"].str.lower() == water_type.lower()) &
            (self.water["Unit"].str.lower() == unit.lower()) &
            (self.water["Year"] == year)
        ]
        if match.empty:
            return None
        row = match.iloc[0]
        return float(row["kg CO2e"]), row["Source"]
    
    def get_waste_factor(self, waste_type: str, unit: str, year: int) -> Optional[Tuple[float, str]]:
        match = self.waste[
            (self.waste["Waste Type"].str.lower() == waste_type.lower()) &
            (self.waste["Unit"].str.lower() == unit.lower()) &
            (self.waste["Year"] == year)
        ]
        if match.empty:
            return None
        row = match.iloc[0]
        return float(row["kg CO2e"]), row["Source"]
    
    def list_supported_years(self) -> List[int]:
        return sorted(self.electricity["Year"].unique().tolist())
    
    def list_factors_by_year(self, year: int) -> Dict[str, Any]:
        return {
            "year": year,
            "electricity_factors": self.electricity[self.electricity["Year"] == year].to_dict(orient="records"),
            "fuel_factors": self.fuels[self.fuels["Year"] == year].to_dict(orient="records"),
            "vehicle_factors": self.vehicles[self.vehicles["Year"] == year].to_dict(orient="records"),
            "water_factors": self.water[self.water["Year"] == year].to_dict(orient="records"),
            "waste_factors": self.waste[self.waste["Year"] == year].to_dict(orient="records"),
        }


DEFRA_LIBRARY = DEFRAFactorLibrary()


# =========================
# INTELLIGENT COLUMN DETECTOR
# =========================
class IntelligentColumnDetector:
    """AI-powered column detection using fuzzy matching and pattern recognition."""
    
    @staticmethod
    def similarity_ratio(a: str, b: str) -> float:
        """Calculate similarity between two strings."""
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()
    
    @staticmethod
    def extract_unit_from_header(header: str) -> Optional[str]:
        """Extract unit from column header like 'Energy (kWh)' or 'Usage kWh'."""
        # Pattern: (unit) or unit_at_end
        patterns = [
            r'\(([^)]+)\)',  # (kWh)
            r'(?:in|of)\s+([a-zA-Z0-9.\s]+)$',  # in kWh
        ]
        for pattern in patterns:
            match = re.search(pattern, header, re.IGNORECASE)
            if match:
                unit_text = match.group(1).strip()
                normalized = IntelligentColumnDetector.normalize_unit(unit_text)
                if normalized:
                    return normalized
        return None
    
    @staticmethod
    def normalize_unit(unit_text: str) -> Optional[str]:
        """Normalize unit text to standard format."""
        unit_lower = unit_text.strip().lower()
        return UNIT_ALIASES.get(unit_lower, None)
    
    @staticmethod
    def detect_column_intent(header: str, sample_values: List[str]) -> Tuple[ColumnIntent, float]:
        """
        Detect the intent of a column using header and sample values.
        Returns (ColumnIntent, confidence_score 0-1)
        """
        header_lower = header.lower()
        
        # Check header keywords
        amount_score = max([IntelligentColumnDetector.similarity_ratio(header_lower, kw) for kw in AMOUNT_KEYWORDS], default=0)
        unit_score = max([IntelligentColumnDetector.similarity_ratio(header_lower, kw) for kw in UNIT_KEYWORDS], default=0)
        category_score = max([IntelligentColumnDetector.similarity_ratio(header_lower, kw) for kw in CATEGORY_KEYWORDS], default=0)
        date_score = max([IntelligentColumnDetector.similarity_ratio(header_lower, kw) for kw in DATE_KEYWORDS], default=0)
        vehicle_score = max([IntelligentColumnDetector.similarity_ratio(header_lower, kw) for kw in VEHICLE_KEYWORDS], default=0)
        
        # Check sample values
        if sample_values:
            sample_str = " ".join([str(v).lower() for v in sample_values[:5]])
            
            # Check for numeric patterns (amount)
            if any(re.match(r'^\d+\.?\d*$', str(v).strip()) for v in sample_values if v):
                amount_score = max(amount_score, 0.8)
            
            # Check for unit patterns
            if any(str(v).lower() in ['kwh', 'kg', 'tonnes', 'litres', 'm3'] for v in sample_values if v):
                unit_score = max(unit_score, 0.9)
            
            # Check for date patterns
            if any(re.match(r'^\d{1,2}[/-]\d{1,2}|^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)', str(v).lower()) for v in sample_values if v):
                date_score = max(date_score, 0.8)
            
            # Check for vehicle patterns
            if any(str(v).lower() in list(VEHICLE_TYPE_ALIASES.keys()) for v in sample_values if v):
                vehicle_score = max(vehicle_score, 0.9)
            
            # Check for category patterns
            if any(str(v).lower() in list(CATEGORY_ALIASES.keys()) for v in sample_values if v):
                category_score = max(category_score, 0.9)
        
        scores = {
            ColumnIntent.AMOUNT: amount_score,
            ColumnIntent.UNIT: unit_score,
            ColumnIntent.CATEGORY: category_score,
            ColumnIntent.DATE_PERIOD: date_score,
            ColumnIntent.VEHICLE_TYPE: vehicle_score,
        }
        
        best_intent = max(scores, key=scores.get)
        best_score = scores[best_intent]
        
        return best_intent, best_score if best_score > 0.3 else 0.0
    
    @staticmethod
    def auto_detect_columns(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Intelligent column detection for entire DataFrame.
        Returns mapping of detected columns with confidence.
        """
        detected = {}
        
        for col in df.columns:
            sample_values = df[col].dropna().head(5).tolist()
            intent, confidence = IntelligentColumnDetector.detect_column_intent(col, sample_values)
            
            unit_from_header = IntelligentColumnDetector.extract_unit_from_header(col)
            
            detected[col] = {
                "intent": intent,
                "confidence": confidence,
                "extracted_unit": unit_from_header,
                "sample_values": sample_values[:3]
            }
        
        return detected


# =========================
# CSV VALIDATION ENGINE
# =========================
class CSVValidator:
    """Pre-upload validation and remediation."""
    
    @staticmethod
    def validate_csv(df: pd.DataFrame, required_intent: ColumnIntent = None) -> Dict[str, Any]:
        """
        Validate CSV structure before processing.
        """
        issues = []
        warnings = []
        column_detection = IntelligentColumnDetector.auto_detect_columns(df)
        
        # Check for completely empty rows
        empty_rows = df.isna().sum(axis=1) == len(df.columns)
        if empty_rows.any():
            issues.append(f"Found {empty_rows.sum()} completely empty rows")
        
        # Check for numeric columns (amount detection)
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if len(numeric_cols) == 0:
            warnings.append("No numeric columns detected. Amount values may fail to parse.")
        
        # Validation summary
        high_confidence_cols = [col for col, det in column_detection.items() if det["confidence"] > 0.7]
        medium_confidence_cols = [col for col, det in column_detection.items() if 0.3 < det["confidence"] <= 0.7]
        low_confidence_cols = [col for col, det in column_detection.items() if 0.0 < det["confidence"] <= 0.3]
        
        return {
            "is_valid": len(issues) == 0,
            "issues": issues,
            "warnings": warnings,
            "column_detection": column_detection,
            "high_confidence_columns": high_confidence_cols,
            "medium_confidence_columns": medium_confidence_cols,
            "low_confidence_columns": low_confidence_cols,
            "total_rows": len(df),
            "suggested_actions": [
                f"Confirm {len(medium_confidence_cols)} medium-confidence columns" if medium_confidence_cols else None,
                f"Review {len(low_confidence_cols)} low-confidence columns" if low_confidence_cols else None,
                f"Remove {empty_rows.sum()} empty rows" if empty_rows.any() else None
            ]
        }
    
    @staticmethod
    def suggest_column_mapping(df: pd.DataFrame) -> Dict[str, str]:
        """
        Suggest standard column names for detected intents.
        """
        column_detection = IntelligentColumnDetector.auto_detect_columns(df)
        mapping = {}
        
        for col, det in column_detection.items():
            intent = det["intent"]
            confidence = det["confidence"]
            
            if intent == ColumnIntent.AMOUNT and confidence > 0.5:
                mapping[col] = "amount"
            elif intent == ColumnIntent.UNIT and confidence > 0.5:
                mapping[col] = "unit"
            elif intent == ColumnIntent.CATEGORY and confidence > 0.5:
                mapping[col] = "category"
            elif intent == ColumnIntent.DATE_PERIOD and confidence > 0.5:
                mapping[col] = "reporting_period"
            elif intent == ColumnIntent.VEHICLE_TYPE and confidence > 0.5:
                mapping[col] = "vehicle_type"
        
        return mapping


# =========================
# CSV TEMPLATE GENERATOR
# =========================
class CSVTemplateGenerator:
    """Generate branded CSV templates."""
    
    @staticmethod
    def generate_template(company_name: str = "Your Company", brand_color: str = "#2E7D32") -> pd.DataFrame:
        """Generate a branded CSV template with examples."""
        template_data = {
            "reporting_period": [
                "April 2024", "April 2024", "April 2024",
                "April 2024", "April 2024", "April 2024"
            ],
            "category": [
                "Electricity", "Electricity", "Natural Gas",
                "Business Travel", "Business Travel", "Business Travel"
            ],
            "item_name": [
                "UK Grid Electricity", "UK Grid Electricity", "Natural Gas",
                "Fleet Vehicle - VW ID3", "Fleet Vehicle - BMW 330e", "HGV - Mercedes Actros"
            ],
            "amount": [5200, 3100, 850, 1200, 950, 450],
            "unit": ["kWh", "kWh", "kWh (Gross CV)", "vehicle_km", "vehicle_km", "vehicle_km"],
            "notes": [
                "Main office consumption",
                "Warehouse consumption",
                "Heating system",
                "Electric vehicle - low emissions",
                "Hybrid vehicle - mixed fuel",
                "Heavy goods vehicle - transport"
            ]
        }
        
        df = pd.DataFrame(template_data)
        return df
    
    @staticmethod
    def export_template_to_csv(company_name: str = "Your Company") -> bytes:
        """Export template as CSV bytes."""
        df = CSVTemplateGenerator.generate_template(company_name)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        return csv_buffer.getvalue().encode('utf-8')
    
    @staticmethod
    def generate_template_with_instructions() -> str:
        """Generate template with embedded instructions."""
        instructions = """
# Carbon Calculator - Data Upload Template
# Generated: {date}
# Company: Your Company Name
# Reporting Period: April 2024

## INSTRUCTIONS:
# 1. Fill in the "reporting_period" column with the month/year you're reporting for
# 2. Select a "category" from: Electricity, Natural Gas, Business Travel, Water, Waste
# 3. Enter "item_name" (e.g., "Office Electricity", "VW ID3 Vehicle")
# 4. Enter the numeric "amount" (consumption/usage value)
# 5. Specify the "unit" (e.g., kWh, kg, vehicle_km, tonnes)
# 6. Optional: Add notes for reference

## SUPPORTED CATEGORIES & UNITS:

# ELECTRICITY:
# - Unit: kWh
# - Example: reporting_period=April 2024, category=Electricity, item_name=Office, amount=5000, unit=kWh

# NATURAL GAS:
# - Unit: kWh (Gross CV), kWh (Net CV), cubic metres, tonnes
# - Example: reporting_period=April 2024, category=Natural Gas, amount=850, unit=kWh (Gross CV)

# BUSINESS TRAVEL:
# - Unit: vehicle_km
# - Vehicle Types: Petrol car, Diesel car, Electric car (BEV), Hybrid (HEV/PHEV), HGV (44 tonnes), Bus, Flight (short/medium/long-haul), Rail
# - Example: reporting_period=April 2024, category=Business Travel, item_name=VW ID3, amount=1200, unit=vehicle_km

# WATER:
# - Unit: cubic metres, million litres
# - Example: reporting_period=April 2024, category=Water, amount=120, unit=cubic metres

# WASTE:
# - Unit: kg, tonnes
# - Waste Types: General waste, Landfill waste, Recycling, Incineration
# - Example: reporting_period=April 2024, category=Waste, item_name=General waste, amount=250, unit=kg

reporting_period,category,item_name,amount,unit,notes
April 2024,Electricity,Office Electricity,5200,kWh,Main office building
April 2024,Electricity,Warehouse,3100,kWh,Secondary location
April 2024,Natural Gas,Building Heating,850,kWh (Gross CV),Gas boiler
April 2024,Business Travel,VW ID3,1200,vehicle_km,Electric company vehicle
April 2024,Business Travel,BMW 330e,950,vehicle_km,Hybrid company vehicle
April 2024,Business Travel,Mercedes Actros HGV,450,vehicle_km,Heavy goods transport
""".format(date=datetime.now().strftime("%Y-%m-%d"))
        
        return instructions


# =========================
# NORMALIZATION HELPERS
# =========================
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


def validate_year(year, default=DEFAULT_FACTOR_YEAR):
    if year is None:
        return default
    try:
        year = int(float(year))
    except Exception:
        raise ValueError(f"year must be an integer. Supported: {SUPPORTED_YEARS}")
    if year not in SUPPORTED_YEARS:
        raise ValueError(f"year {year} not supported. Supported: {SUPPORTED_YEARS}")
    return year


def safe_text(value):
    if value is None:
        return None
    value = str(value).strip()
    if value == "" or value.lower() == "nan":
        return None
    return value


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
# OUTPUT BUILDER
# =========================
def build_result_row(
    category, sub_category, item_name, scope, input_amount, input_unit, normalized_unit,
    emission_factor, factor_year, emissions_kg, data_source="DEFRA", factor_name=None,
    defra_reference=None, original_category=None, mapping_source=None, mapping_confidence=None,
):
    calculation_basis = "exact" if mapping_source == "category" else "estimated"
    
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
        "original_category": original_category,
        "mapping_source": mapping_source,
        "mapping_confidence": mapping_confidence,
        "calculation_basis": calculation_basis
    }])


# =========================
# CALCULATORS
# =========================
def calculate_electricity_emissions(
    country, unit, amount, year=DEFAULT_FACTOR_YEAR,
    original_category=None, mapping_source=None, mapping_confidence=None
):
    country = validate_text_input(country, "country")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)
    year = validate_year(year)

    unit_normalized = normalize_unit(unit)
    
    result = DEFRA_LIBRARY.get_electricity_factor(country, unit_normalized, year)
    
    if not result:
        raise ValueError(
            f"No matching electricity factor found for Country='{country}', Unit='{unit_normalized}', Year={year}. "
            f"Supported years: {SUPPORTED_YEARS}"
        )
    
    factor, defra_source = result
    emissions_kg = amount * factor

    return build_result_row(
        category="Electricity",
        sub_category="Electricity generated",
        item_name=country,
        scope="Scope 2",
        input_amount=amount,
        input_unit=unit,
        normalized_unit=unit_normalized,
        emission_factor=factor,
        factor_year=year,
        emissions_kg=emissions_kg,
        data_source=defra_source,
        factor_name=f"{country} / {unit_normalized}",
        defra_reference=defra_source,
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence,
    )


def calculate_fuel_emissions(
    fuel, unit, amount, year=DEFAULT_FACTOR_YEAR,
    original_category=None, mapping_source=None, mapping_confidence=None
):
    fuel = validate_text_input(fuel, "fuel")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)
    year = validate_year(year)

    unit_normalized = normalize_unit(unit)
    
    result = DEFRA_LIBRARY.get_fuel_factor(fuel, unit_normalized, year)
    
    if not result:
        raise ValueError(
            f"No matching fuel factor found for Fuel='{fuel}', Unit='{unit_normalized}', Year={year}. "
            f"Supported years: {SUPPORTED_YEARS}"
        )
    
    factor, defra_source = result
    emissions_kg = amount * factor

    return build_result_row(
        category="Fuel",
        sub_category="Gaseous fuels",
        item_name=fuel,
        scope="Scope 1",
        input_amount=amount,
        input_unit=unit,
        normalized_unit=unit_normalized,
        emission_factor=factor,
        factor_year=year,
        emissions_kg=emissions_kg,
        data_source=defra_source,
        factor_name=f"{fuel} / {unit_normalized}",
        defra_reference=defra_source,
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence,
    )


def calculate_vehicle_emissions(
    vehicle_type, unit, amount, year=DEFAULT_FACTOR_YEAR,
    original_category=None, mapping_source=None, mapping_confidence=None
):
    vehicle_type = validate_text_input(vehicle_type, "vehicle_type")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)
    year = validate_year(year)

    unit_normalized = normalize_unit(unit)
    vehicle_normalized = normalize_vehicle_type(vehicle_type)

    if not vehicle_normalized:
        raise ValueError(
            f"Unknown vehicle type: '{vehicle_type}'. "
            f"Supported: Petrol car, Diesel car, Electric car (BEV), Hybrid (HEV/PHEV), "
            f"HGV (44 tonnes), Light commercial vehicle, Bus, Flight (short/medium/long-haul), Rail, Hotel stay."
        )
    
    result = DEFRA_LIBRARY.get_vehicle_factor(vehicle_normalized, unit_normalized, year)
    
    if not result:
        raise ValueError(
            f"No matching vehicle factor found for '{vehicle_normalized}' with unit '{unit_normalized}' in year {year}. "
            f"Supported years: {SUPPORTED_YEARS}"
        )
    
    factor, defra_source = result
    emissions_kg = amount * factor

    return build_result_row(
        category="Business Travel",
        sub_category="Business travel",
        item_name=vehicle_normalized,
        scope="Scope 3",
        input_amount=amount,
        input_unit=unit,
        normalized_unit=unit_normalized,
        emission_factor=factor,
        factor_year=year,
        emissions_kg=emissions_kg,
        data_source=defra_source,
        factor_name=f"{vehicle_normalized} / {unit_normalized}",
        defra_reference=defra_source,
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence,
    )


def calculate_water_emissions(
    water_type, unit, amount, year=DEFAULT_FACTOR_YEAR,
    original_category=None, mapping_source=None, mapping_confidence=None
):
    water_type = validate_text_input(water_type, "water_type")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)
    year = validate_year(year)

    unit_normalized = normalize_unit(unit)
    
    result = DEFRA_LIBRARY.get_water_factor(water_type, unit_normalized, year)
    
    if not result:
        raise ValueError(
            f"No matching water factor found for Type='{water_type}', Unit='{unit_normalized}', Year={year}. "
            f"Supported years: {SUPPORTED_YEARS}"
        )
    
    factor, defra_source = result
    emissions_kg = amount * factor

    return build_result_row(
        category="Water",
        sub_category="Water supply",
        item_name=water_type,
        scope="Scope 3",
        input_amount=amount,
        input_unit=unit,
        normalized_unit=unit_normalized,
        emission_factor=factor,
        factor_year=year,
        emissions_kg=emissions_kg,
        data_source=defra_source,
        factor_name=f"{water_type} / {unit_normalized}",
        defra_reference=defra_source,
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence,
    )


def calculate_waste_emissions(
    waste_type, unit, amount, year=DEFAULT_FACTOR_YEAR,
    original_category=None, mapping_source=None, mapping_confidence=None
):
    waste_type = validate_text_input(waste_type, "waste_type")
    unit = validate_text_input(unit, "unit")
    amount = validate_amount(amount)
    year = validate_year(year)

    unit_normalized = normalize_unit(unit)
    waste_normalized = normalize_waste_type(waste_type) or waste_type
    
    result = DEFRA_LIBRARY.get_waste_factor(waste_normalized, unit_normalized, year)
    
    if not result:
        raise ValueError(
            f"No matching waste factor found for Type='{waste_normalized}', Unit='{unit_normalized}', Year={year}. "
            f"Supported years: {SUPPORTED_YEARS}"
        )
    
    factor, defra_source = result
    emissions_kg = amount * factor

    return build_result_row(
        category="Waste",
        sub_category="Waste generated",
        item_name=waste_normalized,
        scope="Scope 3",
        input_amount=amount,
        input_unit=unit,
        normalized_unit=unit_normalized,
        emission_factor=factor,
        factor_year=year,
        emissions_kg=emissions_kg,
        data_source=defra_source,
        factor_name=f"{waste_normalized} / {unit_normalized}",
        defra_reference=defra_source,
        original_category=original_category,
        mapping_source=mapping_source,
        mapping_confidence=mapping_confidence,
    )


# =========================
# DISPATCHER WITH SMART MAPPING
# =========================
def calculate_emissions(activity):
    """Route to appropriate calculator with smart mapping."""
    raw_category = safe_text(activity.get("category"))
    canonical_category = normalize_category(raw_category)

    if not canonical_category:
        raise ValueError(
            f"Unsupported or unmapped category: '{raw_category}'. "
            f"Supported: fuel, electricity, water, business travel, waste."
        )

    unit = activity.get("unit")
    amount = activity.get("amount")
    year = activity.get("year", activity.get("factor_year", DEFAULT_FACTOR_YEAR))

    mapping_source = "category"
    mapping_confidence = "high"

    if canonical_category == "fuel":
        fuel = activity.get("fuel") or activity.get("item_name") or "Natural gas"
        return calculate_fuel_emissions(
            fuel=fuel, unit=unit, amount=amount, year=year,
            original_category=raw_category, mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    elif canonical_category == "electricity":
        country = activity.get("country") or activity.get("item_name") or "Electricity: UK"
        unit = unit or "kWh"
        return calculate_electricity_emissions(
            country=country, unit=unit, amount=amount, year=year,
            original_category=raw_category, mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    elif canonical_category == "water":
        water_type = activity.get("water_type") or activity.get("item_name") or "Water supply"
        return calculate_water_emissions(
            water_type=water_type, unit=unit, amount=amount, year=year,
            original_category=raw_category, mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    elif canonical_category == "business travel":
        vehicle_type = activity.get("vehicle_type") or activity.get("item_name")
        if not vehicle_type:
            raise ValueError("Business travel requires vehicle_type (e.g., 'Electric car (BEV)', 'HGV (44 tonnes)')")
        
        return calculate_vehicle_emissions(
            vehicle_type=vehicle_type, unit=unit, amount=amount, year=year,
            original_category=raw_category, mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    elif canonical_category == "waste":
        waste_type = activity.get("waste_type") or activity.get("item_name") or "General waste"
        return calculate_waste_emissions(
            waste_type=waste_type, unit=unit, amount=amount, year=year,
            original_category=raw_category, mapping_source=mapping_source,
            mapping_confidence=mapping_confidence
        )

    raise ValueError(f"Unsupported category: '{raw_category}'")


# =========================
# BATCH PROCESSING WITH AUTO-MAPPING
# =========================
def calculate_emissions_batch_safe(activity_list, auto_map=True):
    """Process batch with optional auto-mapping."""
    results = []
    errors = []
    mapping_log = {}

    for i, activity in enumerate(activity_list):
        try:
            activity_data = activity.copy()
            result = calculate_emissions(activity_data)
            result["row_index"] = i
            results.append(result)
            
            # Log mapping
            item_name = result.iloc[0]["item_name"]
            mapping_log[i] = {
                "input": activity_data,
                "mapped_to": item_name,
                "category": result.iloc[0]["category"],
                "emissions_kgCO2e": float(result.iloc[0]["emissions_kgCO2e"])
            }

        except Exception as e:
            errors.append({
                "row_index": i,
                "row_index_original": activity.get("row_index_original", i + 1),
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
    coverage_percent = round((successful_rows / total_rows) * 100, 2) if total_rows > 0 else 0.0

    data_quality = {
        "total_rows": total_rows,
        "successful_rows": successful_rows,
        "errored_rows": len(errors),
        "coverage_percent": coverage_percent,
    }

    return {
        "report": final_report,
        "total_kgCO2e": total_kg,
        "total_tCO2e": total_t,
        "errors": errors,
        "data_quality": data_quality,
        "mapping_log": mapping_log
    }


# =========================
# FACTOR AUDIT LOG
# =========================
class FactorAuditLog:
    """Generate detailed factor audit trails."""
    
    @staticmethod
    def generate_fleet_audit(
        batch_result: Dict, 
        defra_year: int = DEFAULT_FACTOR_YEAR
    ) -> Dict[str, Any]:
        """Generate audit log for fleet-specific calculations."""
        report_df = batch_result["report"]
        mapping_log = batch_result["mapping_log"]
        
        fleet_summary = {}
        
        for row_idx, entry in mapping_log.items():
            item_name = entry["mapped_to"]
            category = entry["category"]
            emissions = entry["emissions_kgCO2e"]
            
            if item_name not in fleet_summary:
                fleet_summary[item_name] = {
                    "category": category,
                    "total_emissions_kgCO2e": 0,
                    "total_emissions_tCO2e": 0,
                    "defra_factor": None,
                    "defra_year": defra_year,
                    "count": 0,
                    "unit": None
                }
            
            fleet_summary[item_name]["total_emissions_kgCO2e"] += emissions
            fleet_summary[item_name]["total_emissions_tCO2e"] = fleet_summary[item_name]["total_emissions_kgCO2e"] / 1000
            fleet_summary[item_name]["count"] += 1
        
        # Populate factors from report
        for idx, row in report_df.iterrows():
            item_name = row["item_name"]
            if item_name in fleet_summary:
                fleet_summary[item_name]["defra_factor"] = float(row["emission_factor_kgCO2e_per_unit"])
                fleet_summary[item_name]["unit"] = row["normalized_unit"]
        
        return {
            "audit_timestamp": datetime.now().isoformat(),
            "defra_year": defra_year,
            "total_fleet_emissions_kgCO2e": batch_result["total_kgCO2e"],
            "total_fleet_emissions_tCO2e": batch_result["total_tCO2e"],
            "fleet_breakdown": fleet_summary,
            "coverage": batch_result["data_quality"]["coverage_percent"],
            "errors": batch_result["data_quality"]["errored_rows"]
        }
    
    @staticmethod
    def generate_factor_transparency_report(
        batch_result: Dict
    ) -> Dict[str, Any]:
        """Generate factor transparency report showing exact DEFRA mappings."""
        report_df = batch_result["report"]
        
        transparency = {
            "report_generated": datetime.now().isoformat(),
            "total_calculations": len(report_df),
            "factors_used": []
        }
        
        for idx, row in report_df.iterrows():
            transparency["factors_used"].append({
                "input_item": row["item_name"],
                "category": row["category"],
                "defra_factor_kgCO2e_per_unit": float(row["emission_factor_kgCO2e_per_unit"]),
                "defra_year": int(row["factor_year"]),
                "defra_source": row["defra_reference"],
                "unit": row["normalized_unit"],
                "scope": row["scope"],
                "calculation_basis": row["calculation_basis"]
            })
        
        return transparency


# =========================
# LEAD GENERATION FORMATTER
# =========================
class LeadGenFormatter:
    """Format results for lead generation systems (Google Sheets, CRM, etc)."""
    
    @staticmethod
    def format_for_google_sheets(
        batch_result: Dict,
        company_name: str,
        reporting_period: str,
        contact_email: str = None
    ) -> Dict[str, Any]:
        """Format data for Google Sheets integration."""
        
        return {
            "lead_data": {
                "company_name": company_name,
                "reporting_period": reporting_period,
                "contact_email": contact_email,
                "total_co2e_tonnes": round(batch_result["total_tCO2e"], 2),
                "total_co2e_kgCO2e": round(batch_result["total_kgCO2e"], 2),
                "data_quality_score": batch_result["data_quality"]["coverage_percent"],
                "rows_processed": batch_result["data_quality"]["successful_rows"],
                "rows_total": batch_result["data_quality"]["total_rows"],
            },
            "breakdown_by_scope": LeadGenFormatter._calculate_scope_breakdown(batch_result["report"]),
            "breakdown_by_category": LeadGenFormatter._calculate_category_breakdown(batch_result["report"]),
            "generated_at": datetime.now().isoformat(),
            "is_valid_for_reporting": batch_result["data_quality"]["coverage_percent"] > 70
        }
    
    @staticmethod
    def _calculate_scope_breakdown(report_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate emissions by Scope."""
        if report_df.empty:
            return {}
        
        breakdown = {}
        for scope in ["Scope 1", "Scope 2", "Scope 3"]:
            scope_data = report_df[report_df["scope"] == scope]
            if not scope_data.empty:
                breakdown[scope] = round(float(scope_data["emissions_tCO2e"].sum()), 2)
        
        return breakdown
    
    @staticmethod
    def _calculate_category_breakdown(report_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate emissions by category."""
        if report_df.empty:
            return {}
        
        breakdown = {}
        for category in report_df["category"].unique():
            cat_data = report_df[report_df["category"] == category]
            breakdown[category] = round(float(cat_data["emissions_tCO2e"].sum()), 2)
        
        return breakdown
    
    @staticmethod
    def format_for_human_report(
        batch_result: Dict,
        company_name: str,
        reporting_period: str
    ) -> str:
        """Generate human-readable report."""
        
        report = f"""
╔════════════════════════════════════════════════════════════╗
║          CARBON EMISSIONS REPORT                           ║
║          {datetime.now().strftime('%Y-%m-%d')}                          ║
╚════════════════════════════════════════════════════════════╝

COMPANY: {company_name}
REPORTING PERIOD: {reporting_period}
STATUS: {"✓ VALID" if batch_result['data_quality']['coverage_percent'] > 70 else "⚠ INCOMPLETE"}

────────────────────────────────────────────────────────────
TOTAL EMISSIONS:
────────────────────────────────────────────────────────────
Total: {batch_result['total_tCO2e']:.2f} tCO2e ({batch_result['total_kgCO2e']:.2f} kgCO2e)

────────────────────────────────────────────────────────────
BREAKDOWN BY SCOPE:
────────────────────────────────────────────────────────────
"""
        
        scope_breakdown = LeadGenFormatter._calculate_scope_breakdown(batch_result["report"])
        for scope, value in scope_breakdown.items():
            report += f"{scope}: {value:.2f} tCO2e\n"
        
        report += "\n────────────────────────────────────────────────────────────\n"
        report += "BREAKDOWN BY CATEGORY:\n"
        report += "────────────────────────────────────────────────────────────\n"
        
        cat_breakdown = LeadGenFormatter._calculate_category_breakdown(batch_result["report"])
        for category, value in cat_breakdown.items():
            report += f"{category}: {value:.2f} tCO2e\n"
        
        report += f"\n────────────────────────────────────────────────────────────\n"
        report += f"DATA QUALITY:\n"
        report += "────────────────────────────────────────────────────────────\n"
        report += f"Rows Processed: {batch_result['data_quality']['successful_rows']} / {batch_result['data_quality']['total_rows']}\n"
        report += f"Coverage: {batch_result['data_quality']['coverage_percent']:.1f}%\n"
        report += f"Errors: {batch_result['data_quality']['errored_rows']}\n"
        
        if batch_result["errors"]:
            report += "\n────────────────────────────────────────────────────────────\n"
            report += "ERRORS ENCOUNTERED:\n"
            report += "────────────────────────────────────────────────────────────\n"
            for err in batch_result["errors"][:5]:
                report += f"Row {err['row_index_original']}: {err['error']}\n"
        
        return report


# =========================
# FINAL RESPONSE BUILDER
# =========================
def build_final_response(batch_result, company_name: str = None, reporting_period: str = None):
    """Build comprehensive response with all metadata."""
    report_df = batch_result["report"]
    line_items = report_df.to_dict(orient="records") if not report_df.empty else []

    return {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "defra_year": DEFAULT_FACTOR_YEAR,
            "company_name": company_name,
            "reporting_period": reporting_period,
        },
        "totals": {
            "total_kgCO2e": batch_result["total_kgCO2e"],
            "total_tCO2e": batch_result["total_tCO2e"],
        },
        "line_items": line_items,
        "data_quality": batch_result["data_quality"],
        "errors": batch_result["errors"],
        "factor_reference_years_used": list(set([item.get("factor_year") for item in line_items])) if line_items else [],
        "fleet_audit": FactorAuditLog.generate_fleet_audit(batch_result),
        "factor_transparency": FactorAuditLog.generate_factor_transparency_report(batch_result),
        "lead_gen_data": LeadGenFormatter.format_for_google_sheets(batch_result, company_name, reporting_period) if company_name else None,
    }


def make_json_safe(obj):
    """Convert numpy/pandas types to JSON-safe types."""
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


# =========================
# MAIN ENGINE API
# =========================
def run_emissions_engine(request_json: Dict) -> Dict[str, Any]:
    """Main entry point for carbon calculation."""
    activities = request_json.get("activities", [])
    company_name = request_json.get("company_name")
    reporting_period = request_json.get("reporting_period")
    
    batch_result = calculate_emissions_batch_safe(activities)
    final_response = build_final_response(batch_result, company_name, reporting_period)
    return make_json_safe(final_response)


def validate_and_preview_csv(csv_path: str) -> Dict[str, Any]:
    """Validate CSV before processing."""
    df = pd.read_csv(csv_path)
    validation = CSVValidator.validate_csv(df)
    mapping_suggestions = CSVValidator.suggest_column_mapping(df)
    
    return {
        "validation": validation,
        "suggested_mapping": mapping_suggestions,
        "preview_rows": df.head(3).to_dict(orient="records")
    }


def generate_csv_template(company_name: str = "Your Company") -> bytes:
    """Generate and return CSV template."""
    return CSVTemplateGenerator.export_template_to_csv(company_name)


def get_factor_audit_log(year: int = DEFAULT_FACTOR_YEAR) -> Dict[str, Any]:
    """Get complete audit log of DEFRA factors."""
    return DEFRA_LIBRARY.list_factors_by_year(year)


def get_supported_years() -> List[int]:
    """Get supported DEFRA years."""
    return DEFRA_LIBRARY.list_supported_years()


# =========================
# EXAMPLE USAGE
# =========================
if __name__ == "__main__":
    # Example request
    request = {
        "company_name": "Acme Corporation",
        "reporting_period": "April 2024",
        "activities": [
            {
                "category": "Electricity",
                "item_name": "Office",
                "amount": 5200,
                "unit": "kWh",
                "row_index_original": 1
            },
            {
                "category": "Electricity",
                "item_name": "Warehouse",
                "amount": 3100,
                "unit": "kWh",
                "row_index_original": 2
            },
            {
                "category": "Fuel",
                "fuel": "Natural gas",
                "amount": 850,
                "unit": "kWh (Gross CV)",
                "row_index_original": 3
            },
            {
                "category": "Business Travel",
                "item_name": "VW ID3",
                "amount": 1200,
                "unit": "vehicle_km",
                "row_index_original": 4
            },
            {
                "category": "Business Travel",
                "item_name": "BMW 330e",
                "amount": 950,
                "unit": "vehicle_km",
                "row_index_original": 5
            },
            {
                "category": "Business Travel",
                "item_name": "Mercedes Actros",
                "amount": 450,
                "unit": "vehicle_km",
                "row_index_original": 6
            }
        ]
    }
    
    response = run_emissions_engine(request)
    print(json.dumps(response, indent=2))
    
    # Generate human-readable report
    batch_result = calculate_emissions_batch_safe(request["activities"])
    report = LeadGenFormatter.format_for_human_report(
        batch_result,
        request.get("company_name"),
        request.get("reporting_period")
    )
    print("\n" + report)

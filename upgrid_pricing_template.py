from dotenv import load_dotenv
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    average_precision_score,
    confusion_matrix,
    mean_squared_error,
    mean_absolute_error,
    mean_absolute_percentage_error,
    r2_score,
)
import joblib
import pandas_gbq
from openpyxl.styles import PatternFill, Font
from openpyxl import load_workbook
import openpyxl
from thefuzz import fuzz, process
import psutil
import requests
import pytz
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from io import StringIO
from pathlib import Path
from datetime import datetime
import time
import json
import ast
import re
import gc
import os
import warnings
warnings.filterwarnings("ignore")

# =============================================================================

# CONFIGURATION

# =============================================================================

PROJECT_ID = "flex-power"
BERLIN_TZ = pytz.timezone("Europe/Berlin")

# API credentials (should be in environment variables in production)
load_dotenv(r"C:\Users\JerrySetiawan\OneDrive - CFP FlexPower\Work\Data pricing\.env")

BLINDLEISTER_EMAIL = os.getenv("BLINDLEISTER_EMAIL", "")
BLINDLEISTER_PASSWORD = os.getenv("BLINDLEISTER_PASSWORD", "")
ANEMOS_EMAIL = os.getenv("ANEMOS_EMAIL", "")
ANEMOS_PASSWORD = os.getenv("ANEMOS_PASSWORD", "")

TURBINE_REFERENCE_PATH = r"C:\Users\JerrySetiawan\OneDrive - CFP FlexPower\Work\Data pricing\Enervis template\turbine_types_id_enervis_eraseMW.xlsx"
DAY_AHEAD_PRICE_PATH = r"C:\Users\JerrySetiawan\OneDrive - CFP FlexPower\Work\Data pricing\DA price\DA_price.csv"
RMV_PRICE_PATH = r"C:\Users\JerrySetiawan\OneDrive - CFP FlexPower\Work\Data pricing\DA price\rmv_price.csv"
MODEL_BASE_PATH = r"C:\Users\JerrySetiawan\OneDrive - CFP FlexPower\Work\curtailment_prediction\curtailment_model"

# Constants
ROWS_PER_FULL_YEAR = 35000
CUTOFF_DATE_HOURLY = "2025-10-01"
EXCEL_MAX_ROWS = 1000000

# =============================================================================

# HARDCODED TURBINE MAPPINGS

# =============================================================================

TURBINE_HARDCODED_MAP = {
    "V90 MK8 Gridstreamer": "V-90 2.0MW Gridstreamer",
    "V126-3.45MW": "V-126 3.45MW",
    "V-90": "V-90 2.0MW Gridstreamer",
    "Vestas V90 2,00 MW 2.0MW": "V-90 2.0MW Gridstreamer",
    "Vestas V 90 NH 95m 2,00 MW 2.0MW": "V-90 2.0MW Gridstreamer",
    "Vestas V90 2,00 MW": "V-90 2.0MW Gridstreamer",
    "V-112 2.0MW": "V-112 3.3MW",
    "V136-3.6MW": "V-136 3.6MW",
    "V112-3,45": "V-112 3.45MW",
    "V162-5.6 MW": "V-162 5.6MW",
    "V162-6.2 MW": "V-162 6.2MW",
    "Vestas V162": "N-163/6800",
    "V 150-4.2 MW": "V-150 4.2MW (PO)",
    "Vestas V112-3.3 MW MK2A": "V-112 3.3MW",
    "V 80 - 2.0MW / Mode 105.1 dB": "V-80 2.0MW GridStreamer",
    "NTK600/43": "V-44 0.6MW",
    "NTK 600 - 180": "V-44 0.6MW",
    "Nordex N149-5.7 MW": "N-149/5700",
    "Nordex N149-5.X": "N-149/5700",
    "N149-5.7 MW": "N-149/5700",
    "N175-6.8 MW": "N-175/6800",
    "N163-6.8 MW": "N-163/6800",
    "N163-5.7 MW": "N-163/5700",
    "N163-7.0 MW": "N-163/7000",
    "N131/3000 PH134": "N-131/3000",
    "N149/4.0-4.5 NH164": "N-149/4500",
    "N117/2400R91 2400.0": "N-117/2400",
    "N 90-2.5": "N-90/2500",
    "Nordex N149 4500": "N-149/4500",
    "Nordex N131 3600": "N-131/3600",
    "N 117-2.4": "N-117/2400",
    "NordexN131 3300": "N-131/3300",
    "Vestas V90 2.0MW": "V-90 2.0MW Gridstreamer",
    "V-90 MK1-6": "V-90 2.0MW Gridstreamer",
    "V150-4.2 4.2MW": "V-150 4.2MW (PO)",
    "V150-4.2": "V-150 4.2MW (PO)",
    "V117-3.3/3.45MWBWC": "V-117 3.45MW",
    "E-66 1.8MW": "E-66/18.70",
    "E-66": "E-66/18.70",
    "E 53-0.81": "E-53 0.8MW",
    "E-82": "E-82 E2 2.0MW",
    "E-58 1.0MW": "E-58/10.58",
    "E-58": "E-58/10.58",
    "V-80 2.0MW": "V-80 2.0MW GridStreamer",
    "V-80": "V-80 2.0MW GridStreamer",
    "v80 2.0MW": "V-80 2.0MW GridStreamer",
    "V150 4.2": "V-150 4.2MW (PO)",
    "E-138 EP3 E2-HAT-160-ES-C-01": "E-138 EP3 4.2MW",
    "N 131-3300": "N-131/3300",
    "N163/5.X": "N-163/5700",
    "Nordex N117/3600": "N-117/3600",
    "N117/3.6": "N-117/3600",
    "N-117 3150": "N-117/3000",
    "N133 / 4.8 TS110": "N-133/4800",
    "N149/5.7": "N-149/5700",
    "Vensys 77": "77/1500",
    "Senvion 3.4M104": "3.4M104",
    "Senvion 3.2M": "3.2M114",
    "Senvion 3.0M114": "3.2M114",
    "3.2M123": "3.2M122",
    "REpower 3.4M 104 3.37MW": "3.4M122",
    "REpower 3.4M 104": "3.4M122",
    "Senvion 3.2M": "3.4M122",
    "E-141 EP4 4,2 MW": "E-141 EP4 4.2MW",
    "E-70 E4-2/CS 82 a 2.3MW": "E-70 E4 2.3MW",
    "E115 EP3  E3 4.2MW": "E-115 EP3 4.2MW",
    "E115 EP3  E3": "E-115 EP3 4.2MW",
    "E115 EP3 E3": "E-115 EP3 4.2MW",
    "E-53/S/72/3K/02": "E-53 0.8MW",
    "E82 E 2 2.3MW": "E-82 E2 2.3MW",
    "E-40 0.5MW": "E-40/5.40",
    "E 53-0.81 0.8MW": "E-53 0.8MW",
    "NM48/600": "NM 48/600",
    "NEG MICON NM 600/48": "NM 48/600",
    "NM600/48": "NM 48/600",
    "E-70 E4 2300": "E-70 E4 2.3MW",
    "E 82 Serrations": "E-82 E2 2.3MW",
    "E40/540/E1": "E-40/5.40",
    "TW600": "TW 600-43",
    "MM-92": "MM 92 2.05MW",
    "MM92 2.05MW": "MM 92 2.05MW",
    "MM-100": "MM 100 2.0MW",
    "MM-82": "MM 82 2.05MW",
    "MD-77": "MD 77 1.5MW",
    "SWT-3.2": "SWT-3.2-113",
    "GE-5.5": "GE 5.5-158",
    "GE-3.6": "GE 3.6-137",
    "AN62 1,3h": "1300/62",
    "N-117 Gamma": "N-117/2400",
    "AN600": "600/44",
    "AN76 2,0": "2000/76",
    "GE B1500": "GE 1.5sl",
    "Enercon-40": "E-40/5.40",
    "M 1500-600": "M1500-600",
    "GE Wind Energy 1.5 SL": "GE 1.5sl",
    "N-149/4500": "Nordex N149 4500.0",
    "Nordex N131 3600.0": "N-131/3600",
    "NordexN131 3300.0": "N-131/3000",
    "Vestas V150 4.2MW": "V-150 4.2MW (PO)",
    "V112 Mk2": "V-112 3.3MW",
    "MM92": "MM92/2050",
    "N149/5.7 TCS164": "N149/5.7 TCS164",
    "N149/4.5 TCS164": "N-149/4500",
    "N149-4,5MW": "N-149/4500",
    "V117-3,6MW": "V-117 3.45MW",
    "V117- 3,45 MW": "V-117 3.45MW",
    "Vestas V90 - 2,0 MW": "V-90 2.0MW Gridstreamer",
    "3.4M 98.0": "3.4M104",
    "eno92": "eno 92",
    "E53 800kW": "E-53 0.8MW",
    "Vestas V126 - 3,3 MW": "V-126 3.3MW",
    "Vestas V162-6.2": "V-162 6.2MW",
    "N163/6.X TCS164": "N-163/6800",
    "Nordex N100": "N-100/2500",
    "N149/4.0-4.5 Delta 4000": "N-149/4500",
    "N149/4.5 TS 125": "N-149/4500",
    "N149/5.x TS125": "N-149/5700",
    "N117/120 2400.0": "N-117/2400",
    "N117/120 2400": "N-117/2400",
    "SE 3.2M-114": "3.2M114",
    "N163/5.X,": "N-163/5700",
    "N149/5.X TS125-04 5700": "N-149/5700",
    "Delta 4000 N149/5.X": "N-149/5700",
    "N149-5.7": "N-149/5700",
    "V150-4,2": "V-150 4.2MW (PO)",
    "N149/4.0-4.5": "N149/4500",
    "NM48/750": "NM 48/600",
    "V162/6.2MW": "V162/6200",
    "N131": "N-131/3900",
    "V 150 En Ventus 5.6MW": "V-150 5.6MW",
    "V172-7.2 7.2MW": "V-172 7.2MW",
    "V90/2,0 MW 2.0MW": "V-90 2.0MW Gridstreamer",
    "V80": "V-150 5.6MW",
    "E-115 E2": "E-115 3.2MW",
    "E-70 E4": "E-70 E4 2.3MW",
    "Repower MD 77": "MD 77 1.5MW",
    "V90-2.0MW": "V-90 2.0MW Gridstreamer",
    "E-101": "E-101 3.05MW",
    "E-115 EP3": "E-115 3.0MW",
    "V90": "V-90 2.0MW Gridstreamer",
    "E-70": "E-70 E4 2.3MW",
    "MD 77": "MD 77 1.5MW",
    "V90/2MW 2.0MW": "V-90 2.0MW Gridstreamer",
    "N163/5.X TS118": "N-163/5700",
    "E-92": "E-92 2.35MW",
    "V126": "V-126 3.45MW",
    "N149": "N-149/4500",
    "E 82": "E-82 E2 2.3MW",
    "E-115": "E-115 3.0MW",
    "V 150": "V-150 4.2MW (PO)",
    "V136-4.2": "V-136 4.2MW",
    "V117": "V-117 3.3MW",
    "V112-3.45": "V-112 3.45MW",
    "N163/6.X": "N-163/6800",
    "E66": "E-66/20.70",
    "E160-5.56": "E-160 EP5 E3 5.56MW",
    "E138-4.2": "E-138 EP3 4.2MW",
}
# =============================================================================

# UTILITY FUNCTIONS

# =============================================================================


def check_memory_usage() -> float:
    """Return current memory usage in MB"""

    process = psutil.Process(os.getpid())

    memory_info = process.memory_info()

    return memory_info.rss / 1024**2


def ram_check():
    """Print current memory usage"""

    print(f"Memory usage: {check_memory_usage():.2f} MB")


def convert_date_or_keep_string(date: Any) -> str:
    """Convert date to string format or keep original if conversion fails"""

    try:

        date_obj = pd.to_datetime(date, dayfirst=True, errors="raise")

        return date_obj.strftime("%Y-%m-%d")

    except (ValueError, TypeError):

        return str(date)


def is_number(val: Any) -> bool:
    """Check if value can be converted to float"""

    try:

        float(val)

        return True

    except (ValueError, TypeError):

        return False


def ensure_and_reorder(df: pd.DataFrame, order: List[str]) -> pd.DataFrame:
    """Ensure columns exist in DataFrame and reorder them"""

    missing_cols = [col for col in order if col and col not in df.columns]
    for col in missing_cols:
        df[col] = None
    valid_order = [col for col in order if col]
    return df[valid_order]


def print_header(title: str):
    """Print formatted section header"""
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


# =============================================================================

# DATA LOADING & VALIDATION

# =============================================================================


def load_bigquery_fixings(project_id: str) -> pd.DataFrame:
    """Load fixing prices from BigQuery"""

    query = """

    SELECT *

    FROM `flex-power.sales.origination_fixings`

    """

    df = pandas_gbq.read_gbq(query, project_id=project_id)

    df["Tenor"] = df["Tenor"].astype(str)

    print(f"✅ Loaded {len(df)} fixing records from BigQuery")

    return df


def load_stammdaten(path: Path) -> pd.DataFrame:
    """Load and validate master data (stammdaten) from Excel"""

    df = pd.read_excel(path, sheet_name="stammdaten", engine="openpyxl")

    # Clean column names

    df.columns = df.columns.str.strip()

    # Convert malo to string

    df["malo"] = (
        df["malo"]
        .apply(
            lambda x: (
                str(int(x)) if isinstance(x, (float, int)) and pd.notna(x) else str(x)
            )
        )
        .str.strip()
    )

    # Convert MaStR ID to string

    if "Marktstammdatenregister-ID" in df.columns:

        df["Marktstammdatenregister-ID"] = (
            df["Marktstammdatenregister-ID"].astype(str).str.strip()
        )

        df.rename(columns={"Marktstammdatenregister-ID": "unit_mastr_id"}, inplace=True)

    # Remove rows without malo

    df.dropna(subset=("malo",), axis=0, inplace=True)

    print(f"✅ Loaded {len(df)} units from stammdaten")

    return df


def load_day_ahead_prices(path: str) -> pd.DataFrame:
    """Load day-ahead prices and convert to Berlin timezone"""

    df = pd.read_csv(path)

    df["delivery_start__utc_"] = pd.to_datetime(df["delivery_start__utc_"], utc=True)

    df["time_berlin"] = df["delivery_start__utc_"].dt.tz_convert(BERLIN_TZ)

    df["naive_time"] = df["time_berlin"].dt.tz_localize(None)

    # Group and average

    df_avg = df.groupby("naive_time", as_index=False)["dayaheadprice"].mean()

    df_avg = df_avg.rename(columns={"naive_time": "time_berlin"})

    df_avg = df_avg.drop_duplicates(subset=["time_berlin", "dayaheadprice"])

    print(f"✅ Loaded {len(df_avg)} day-ahead price records")

    return df_avg


def load_rmv_prices(path: str) -> pd.DataFrame:
    """Load RMV prices"""

    df = pd.read_csv(path)

    df["tech"] = df["tech"].str.strip().str.upper().astype("category")

    print(f"✅ Loaded {len(df)} RMV price records")

    return df


# =============================================================================

# EEG CATEGORY ASSIGNMENT

# =============================================================================


def set_category_based_on_conditions(df_assets: pd.DataFrame) -> pd.DataFrame:
    """

    Assign EEG rules and category based on technology, capacity, and commissioning date
    Categories:

    - PV_no_rules / PV_rules

    - WIND_no_rules / WIND_rules

    """

    df = df_assets.copy()

    # Parse commissioning date

    df["INB_date"] = pd.to_datetime(df["INB"], dayfirst=True, errors="coerce")

    df["INB_year"] = df["INB_date"].dt.year

    # Define EEG rule conditions

    conditions = [
        # Empty INB
        df["INB"].isna() | (df["INB"] == ""),
        # WIND >=3000 kW, 2016-2020
        (df["tech"] == "WIND")
        & (df["net_power_kw_unit"] >= 3000)
        & (df["INB_year"] >= 2016)
        & (df["INB_year"] < 2021),
        # PV >=500 kW, 2016-2020
        (df["tech"] == "PV")
        & (df["net_power_kw_unit"] >= 500)
        & (df["INB_year"] >= 2016)
        & (df["INB_year"] < 2021),
        # >=500 kW, 2021-2022
        (df["net_power_kw_unit"] >= 500)
        & (df["INB_year"] >= 2021)
        & (df["INB_year"] < 2023),
        # >=100 kW, >=2023
        (df["net_power_kw_unit"] >= 100) & (df["INB_year"] >= 2023),
    ]

    choices = ["rules", "6h rules", "6h rules", "4h rules", "4_3_2_1 rules"]

    df["EEG"] = np.select(conditions, choices, default="no rules")

    df = df.drop(columns=["INB_date", "INB_year"])

    # Set category

    df["category"] = df.apply(
        lambda row: (
            "PV_no_rules"
            if row["tech"] == "PV" and row["EEG"] == "no rules"
            else (
                "PV_rules"
                if row["tech"] == "PV"
                else (
                    "WIND_no_rules"
                    if row["tech"] == "WIND" and row["EEG"] == "no rules"
                    else "WIND_rules"
                )
            )
        ),
        axis=1,
    )

    print("✅ EEG categories assigned")

    return df


# =============================================================================

# FIXING VALUES EXTRACTION

# =============================================================================


def get_fix_value(
    df_fixings: pd.DataFrame, tech: str, variable: str, year: str
) -> float:
    """Extract single fixing value for technology, variable, and year"""

    sel = df_fixings[
        (df_fixings["Technology"] == tech)
        & (df_fixings["Variable"] == variable)
        & (df_fixings["Tenor"] == year)
    ]

    # Prefer non-zero new_Fixing, else EUR_MWh

    s_new = sel["new_Fixing"].replace(0, np.nan).dropna()

    if not s_new.empty:

        return float(s_new.iloc[0])

    s_eur = sel["EUR_MWh"].dropna()

    return float(s_eur.iloc[0]) if not s_eur.empty else np.nan


def extract_all_fixings(
    df_fixings: pd.DataFrame, year: str = "2026"
) -> Dict[str, float]:
    """Extract all required fixing values for a given year"""

    fixings = {
        "bc_pv": get_fix_value(df_fixings, "PV", "Balancing Cost", year),
        "bc_wind": get_fix_value(df_fixings, "Wind", "Balancing Cost", year),
        "tc_pv": get_fix_value(df_fixings, "PV", "Trading Convenience", year),
        "tc_wind": get_fix_value(df_fixings, "Wind", "Trading Convenience", year),
        "cv_pv_no": get_fix_value(
            df_fixings, "PV", "Curtailment Value without Rule", year
        ),
        "cv_pv_yes": get_fix_value(
            df_fixings, "PV", "Curtailment Value with any Rule", year
        ),
        "cv_w_no": get_fix_value(
            df_fixings, "Wind", "Curtailment Value without Rule", year
        ),
        "cv_w_yes": get_fix_value(
            df_fixings, "Wind", "Curtailment Value with any Rule", year
        ),
    }

    # Validate

    for name, value in fixings.items():

        if np.isnan(value):

            print(f"⚠️ Warning: {name} was not successfully fetched")

        else:

            print(f"✅ {name}: {value}")

    return fixings


def apply_fixings_to_stammdaten(
    df_stamm: pd.DataFrame, fixings: Dict[str, float]
) -> pd.DataFrame:
    """Apply fixing values to stammdaten DataFrame"""

    df = df_stamm.copy()

    # Create masks

    m_pv = df["tech"].str.upper().eq("PV")

    m_wind = df["tech"].str.upper().eq("WIND")

    m_no = df["EEG"].str.contains("no rules", case=False, na=False)

    # Assign balancing cost

    df.loc[m_pv, "Balancing Cost"] = fixings["bc_pv"]

    df.loc[m_wind, "Balancing Cost"] = fixings["bc_wind"]

    # Assign trading convenience

    df.loc[m_pv, "Trading Convenience"] = fixings["tc_pv"]

    df.loc[m_wind, "Trading Convenience"] = fixings["tc_wind"]

    # Assign curtailment value

    df.loc[m_pv & m_no, "Curtailment Value"] = fixings["cv_pv_no"]

    df.loc[m_pv & ~m_no, "Curtailment Value"] = fixings["cv_pv_yes"]

    df.loc[m_wind & m_no, "Curtailment Value"] = fixings["cv_w_no"]

    df.loc[m_wind & ~m_no, "Curtailment Value"] = fixings["cv_w_yes"]

    # Calculate weighted curtailment value per malo

    df_curt = (
        df.groupby(["malo"], dropna=False)
        .agg(
            Curtailment_value_weighted=(
                "Curtailment Value",
                lambda x: np.average(x, weights=df.loc[x.index, "net_power_kw_unit"]),
            )
        )
        .reset_index()
    )

    df = pd.merge(df, df_curt, on="malo", how="left")

    print("✅ Fixings applied to stammdaten")

    return df


# =============================================================================

# TURBINE MATCHING (FUZZY)

# =============================================================================


def clean_manufacturer_name(name: str) -> str:
    """Clean manufacturer name by removing common suffixes"""

    if not name or pd.isna(name):

        return ""

    name = str(name).strip()

    # Remove phrases in order from longest to shortest to avoid partial matches

    remove_phrases = [
        "gmbh & co. kg",
        "central europe",
        "deutschland gmbh",
        "energy gmbh",
        "deutschland",
        "gmbh",
        "se",
        "energy",
        "ag",
    ]

    for phrase in remove_phrases:

        # Use word boundaries to avoid removing partial words

        pattern = r"\b" + re.escape(phrase) + r"\b"

        name = re.sub(pattern, "", name, flags=re.IGNORECASE)

    # Normalize multiple spaces to single space

    name = re.sub(r"\s+", " ", name)

    # Normalize manufacturer aliases

    name_lower = name.lower().strip()

    if "ge wind" in name_lower or "ge energy" in name_lower:

        name = "ge"

    elif "neg micon" in name_lower:

        name = "nm"

    elif "repower" in name_lower:

        name = "repower"

    return name.strip().lower()


def clean_turbine_model(name: str) -> str:
    """Clean turbine model name"""

    if not isinstance(name, str) or not name or pd.isna(name):

        return ""

    name = str(name).strip()

    # Remove manufacturer-specific prefixes (order matters: longest first)

    remove_prefixes = [
        "mit serrations",
        "delta 4000",
        "delta4000",
        "neg micon",
        "senvion",
        "enercon",
        "nercon",
        "vensys",
        "vestas",
        "nordex",
        "repower",
        "siemens",
    ]

    for prefix in remove_prefixes:

        pattern = r"\b" + re.escape(prefix) + r"\b"

        name = re.sub(pattern, "", name, flags=re.IGNORECASE)

    # Convert European decimal format (comma) to dot in numbers

    name = re.sub(r"(\d),(\d)", r"\1.\2", name)

    # Remove common suffixes

    suffixes_to_remove = ["turbine", "wind"]

    for suffix in suffixes_to_remove:

        pattern = r"\b" + re.escape(suffix) + r"\b"

        name = re.sub(pattern, "", name, flags=re.IGNORECASE)

    # Remove leading/trailing dashes and spaces

    name = name.strip("- ").strip()

    return name


def prepare_turbine_matching_dataframe(
    df_turbines: pd.DataFrame, df_ref: pd.DataFrame, nan_path: Path, threshold: int = 76,
) -> pd.DataFrame:
    """

    Prepare turbine DataFrame with fuzzy matching to reference database
    Args:
        df_turbines: DataFrame with turbine data
        df_ref: Reference turbine database
        threshold: Fuzzy matching threshold (0-100)
    Returns:

        DataFrame with matched turbine IDs

    """

    df = df_turbines.copy()
    nan_path = Path(nan_path)
    nan_path.parent.mkdir(parents=True, exist_ok=True)

    # Clean names
    df["clean_manufacturer"] = df["manufacturer"].apply(clean_manufacturer_name)
    df["turbine_model_clean"] = df["turbine_model"].apply(clean_turbine_model)
    df["add_turbine"] = df["turbine_model"]
    df["net_power_mw"] = df["net_power_kw"] / 1000

    # Build turbine string based on manufacturer
    vestas_senvion_enercon = [
        "Vestas Deutschland GmbH",
        "Senvion Deutschland GmbH",
        "ENERCON GmbH",
        "VENSYS Energy AG",
        "Enron Wind GmbH",
        "NEG Micon Deutschland GmbH",
    ]

    nordex_repower = [
        "Nordex Energy GmbH",
        "REpower Systems SE",
        "Nordex Germany GmbH",
        "eno energy GmbH",
    ]

    df.loc[df["manufacturer"].isin(vestas_senvion_enercon), "add_turbine"] = (
        df["turbine_model_clean"].astype(str).str.strip()
        + " "
        + df["net_power_mw"].round(3).astype(str)
        + "MW"
    )

    df.loc[df["manufacturer"].isin(nordex_repower), "add_turbine"] = (
        df["turbine_model_clean"].astype(str).str.strip()
        + " "
        + df["net_power_kw"].astype(str)
    )

    df.loc[df["manufacturer"].isin(["REpower Systems SE"]), "add_turbine"] = (
        df["turbine_model_clean"].astype(str).str.strip()
        + " "
        + df["hub_height_m"].astype(str)
    )

    # Prepare reference data with power ratings (rated_power is in kW)

    name_to_power = {}

    if "rated_power" in df_ref.columns:
        name_to_power = df_ref.set_index("name")["rated_power"].to_dict()

    # Prepare hardcoded map keys for fuzzy matching

    hardcoded_keys = list(TURBINE_HARDCODED_MAP.keys())

    # Multi-stage fuzzy matching with power-aware scoring

    def match_with_hardcoded(row):
        # Stage 1: Exact match in hardcoded map (original model name)

        if row["turbine_model"] in TURBINE_HARDCODED_MAP:
            return TURBINE_HARDCODED_MAP[row["turbine_model"]]

        # Stage 2: Exact match in hardcoded map (constructed name)

        if row["add_turbine"] in TURBINE_HARDCODED_MAP:
            return TURBINE_HARDCODED_MAP[row["add_turbine"]]

        if row["turbine_model_clean"] in TURBINE_HARDCODED_MAP:
            return TURBINE_HARDCODED_MAP[row["add_turbine"]]

        # Stage 3: Fuzzy match against df_ref with power-aware scoring

        if isinstance(row["add_turbine"], str) and row["add_turbine"].strip():
            ref_names = df_ref["name"].dropna().unique()

            # Get all candidates above threshold
            matches = process.extract(
                row["add_turbine"], ref_names, scorer=fuzz.token_set_ratio, limit=10
            )

            # Calculate combined score (name + power similarity)

            best_match = None
            best_combined_score = 0

            for match_name, name_score in matches:

                if name_score >= threshold - 10:  # Lower threshold for consideration
                    combined_score = name_score

                    # Add power similarity bonus if power data available

                    if name_to_power and match_name in name_to_power:
                        ref_power_kw = name_to_power[match_name]
                        turbine_power_kw = row["net_power_kw"]

                        if ref_power_kw and turbine_power_kw:
                            # Calculate power similarity score
                            power_diff_pct = (
                                abs(ref_power_kw - turbine_power_kw)
                                / turbine_power_kw
                                * 100
                            )

                            # Power bonus: 0-30 points based on similarity

                            # 0% diff = +30 points, 10% diff = +20 points, 20%
                            # diff = +10 points, >20% = 0 points

                            if power_diff_pct <= 5:
                                power_bonus = 30
                            elif power_diff_pct <= 10:
                                power_bonus = 20
                            elif power_diff_pct <= 20:
                                power_bonus = 10
                            else:
                                power_bonus = 0

                            combined_score = name_score + power_bonus

                    # Update best match if this combined score is higher

                    if combined_score > best_combined_score:
                        best_combined_score = combined_score
                        best_match = match_name

            # Return best match if combined score meets threshold

            if best_match and best_combined_score >= threshold:
                return best_match

        # Stage 4: Fuzzy match against hardcoded map keys (fallback)

        if isinstance(row["add_turbine"], str) and row["add_turbine"].strip():

            # Try constructed name
            match_key, score = process.extractOne(
                row["add_turbine"], hardcoded_keys, scorer=fuzz.token_set_ratio
            )

            if score >= threshold:
                return TURBINE_HARDCODED_MAP[match_key]

            # Try original model name
            match_key_orig, score_orig = process.extractOne(
                row["turbine_model"], hardcoded_keys, scorer=fuzz.token_set_ratio
            )

            if score_orig >= threshold:
                return TURBINE_HARDCODED_MAP[match_key_orig]

        return None

    df["Matched_Turbine_Name"] = df.apply(match_with_hardcoded, axis=1)

    # Map to ID
    name_to_id = df_ref.set_index("name")["id"].to_dict()

    df["Matched_Turbine_ID"] = df["Matched_Turbine_Name"].map(name_to_id)

    df.to_excel(nan_path, index=False)
    print(f"🎐🎐 nan turbine id enervis saved to {nan_path}")

    # Clean up and notif
    matched_count = df["Matched_Turbine_ID"].notna().sum()
    print(f"✅ Matched {matched_count}/{len(df)} turbines to reference database")

    return df


# =============================================================================

# API CLIENTS

# =============================================================================


class BlindleisterAPI:
    """Client for Blindleister API"""

    BASE_URL = "https://api.blindleister.de"

    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.token = None

    def get_token(self) -> str:
        """Get access token"""

        headers = {"accept": "text/plain", "Content-Type": "application/json"}
        json_data = {"email": self.email, "password": self.password}

        response = requests.post(
            f"{self.BASE_URL}/api/v1/authentication/get-access-token",
            headers=headers,
            json=json_data,
        )

        if response.status_code != 200:

            raise Exception(f"Failed to get Blindleister token: {response.status_code}")

        self.token = response.text.strip('"')
        print("✅ Blindleister token obtained")

        return self.token

    def get_market_prices(self, site_ids: List[str], years: List[int]) -> pd.DataFrame:
        """Fetch market prices for multiple sites and years"""

        if not self.token:
            self.get_token()

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

        records = []

        for site_id in site_ids:

            print(f"Fetching market prices for {site_id}...")

            for year in years:

                payload = {"ids": [site_id], "year": year}

                response = requests.post(
                    f"{self.BASE_URL}/api/v1/market-price-atlas-api/get-market-price",
                    headers=headers,
                    json=payload,
                )

                if response.status_code != 200:

                    print(f"⚠️ Failed for {site_id}, {year}: {response.status_code}")

                    continue

                try:

                    result = response.json()

                    for entry in result:

                        entry["year"] = year

                        records.append(entry)

                except Exception as e:

                    print(f"⚠️ Error parsing response for {site_id}, {year}: {e}")

        if not records:

            return pd.DataFrame()

        # Flatten JSON

        df = pd.json_normalize(
            records,
            record_path="months",
            meta=[
                "year",
                "unit_mastr_id",
                "gross_power_kw",
                "energy_source",
                "annual_generated_energy_mwh",
                "benchmark_market_price_eur_mwh",
            ],
            errors="ignore",
        )

        print(f"✅ Fetched {len(df)} market price records from Blindleister")

        return df


class AnemosAPI:
    """Client for Anemos (Enervis) API"""

    BASE_URL = "https://api.anemosgmbh.com"

    AUTH_URL = (
        "https://keycloak.anemosgmbh.com/auth/realms/awis/protocol/openid-connect/token"
    )

    def __init__(self, email: str, password: str):

        self.email = email

        self.password = password

        self.token = None

    def get_token(self) -> str:
        """Get access token"""

        data = {
            "client_id": "webtool_vue",
            "grant_type": "password",
            "username": self.email,
            "password": self.password,
        }

        response = requests.post(self.AUTH_URL, data=data)

        response.raise_for_status()

        self.token = response.json()["access_token"]

        print("✅ Anemos token obtained")

        return self.token

    def get_historical_product_id(self) -> int:
        """Get hist-ondemand product ID"""

        if not self.token:

            self.get_token()

        headers = {"Authorization": f"Bearer {self.token}"}

        response = requests.get(f"{self.BASE_URL}/products_mva", headers=headers)

        response.raise_for_status()

        products = response.json()

        for p in products:

            if "hist-ondemand" in p["mva_product_type"]["name"].lower():

                print(f"✅ Found hist-ondemand product ID: {p['id']}")

                return p["id"]

        raise Exception("hist-ondemand product not found")

    def start_job(self, product_id: int, parkinfo: List[Dict]) -> Optional[str]:
        """Start historical job"""

        if not parkinfo:

            print("⚠️ No parkinfo provided, skipping job")

            return None

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        payload = {"mva_product_id": product_id, "parameters": {"parkinfo": parkinfo}}

        response = requests.post(f"{self.BASE_URL}/jobs", headers=headers, json=payload)

        if response.status_code != 200:

            print(f"❌ Job start failed: {response.text}")

            response.raise_for_status()

        job_uuid = response.json()["uuid"]

        print(f"✅ Job started: {job_uuid}")

        return job_uuid

    def wait_for_job(self, job_uuid: str, poll_interval: int = 10) -> Dict:
        """Poll job status until complete"""

        url = f"{self.BASE_URL}/jobs/{job_uuid}"

        while True:

            headers = {"Authorization": f"Bearer {self.token}"}

            response = requests.get(url, headers=headers)

            if response.status_code == 401:

                print("Token expired, refreshing...")

                self.get_token()

                headers = {"Authorization": f"Bearer {self.token}"}

                response = requests.get(url, headers=headers)

            response.raise_for_status()

            job_info = response.json()

            if isinstance(job_info, list):

                job_info = job_info[0]

            status = job_info.get("status")

            print(f"Job status: {status}")

            if status in ["DONE", "COMPLETED"]:

                return job_info

            elif status in ["FAILED", "CANCELED"]:

                raise Exception(f"Job ended with status: {status}")

            time.sleep(poll_interval)

    def extract_results(self, job_info: Dict) -> List[pd.DataFrame]:
        """Extract results from job info"""

        results = job_info.get("info", {}).get("results", [])

        if not results:

            print("❌ No results found in job")

            return []

        dfs = []

        for result in results:

            turbine_id = result.get("id")

            year_data = result.get("Marktwertdifferenzen")

            if year_data:

                df = pd.DataFrame.from_dict(
                    year_data, orient="index", columns=["Marktwertdifferenz"]
                )

                df.index.name = "Year"

                df = df.reset_index()

                df["id"] = turbine_id

                dfs.append(df)

        print(f"✅ Extracted {len(dfs)} result DataFrames")

        return dfs


# =============================================================================

# PRODUCTION DATA PROCESSING

# =============================================================================


def process_time_column(df: pd.DataFrame) -> pd.DataFrame:
    """

    Process time column - handle both time_berlin and time_utc

    """

    df_result = df.copy()

    if "time_berlin" in df_result.columns:

        df_result["time_berlin"] = pd.to_datetime(
            df_result["time_berlin"], dayfirst=True, errors="coerce"
        )

        df_result["time_berlin"] = df_result["time_berlin"].dt.tz_localize(None)

        print("🥯 Processed time_berlin column")

    elif "time_utc" in df_result.columns:

        df_result["time_utc"] = pd.to_datetime(
            df_result["time_utc"], errors="coerce", dayfirst=True, utc=True
        )

        df_result["time_berlin"] = (
            df_result["time_utc"].dt.tz_convert(BERLIN_TZ).dt.tz_localize(None)
        )

        df_result = df_result.drop(columns="time_utc")
        print("🥯🥯 Converted time_utc to time_berlin")

    else:
        print("⚠️ No time column found")

    return df_result


def expand_hourly_to_quarter_hourly(
    df: pd.DataFrame, cutoff_date: str = CUTOFF_DATE_HOURLY
) -> pd.DataFrame:
    """

    Expand hourly data to quarter-hourly before cutoff date

    Leave data after cutoff unchanged

    """

    df_indexed = df.set_index("time_berlin")

    cutoff = pd.to_datetime(cutoff_date)

    hourly_data = df_indexed[df_indexed.index < cutoff]

    quarter_hourly_data = df_indexed[df_indexed.index >= cutoff]

    if not hourly_data.empty:

        hourly_expanded = hourly_data.resample("15T").ffill()

        result = pd.concat([hourly_expanded, quarter_hourly_data])

    else:

        result = quarter_hourly_data

    return result.sort_index().reset_index()


def filter_production_data_by_completeness(
    df: pd.DataFrame,
    rows_per_full_year: int = ROWS_PER_FULL_YEAR,
    min_rows_per_month: int = 2592,
) -> pd.DataFrame:
    """

    Filter production data to keep only complete/continuous periods
    Logic:

    - Keep full years if available (>=35000 rows/year)

    - Otherwise find longest continuous period (12/24/36 months)

    - Keep only months with >=2592 rows (27 days)

    """

    df_filtered = df[df["time_berlin"].dt.year.isin([2021, 2023, 2024, 2025])].copy()

    filtered_data = []

    for malo, group in df_filtered.groupby("malo"):

        group_filtered = group[
            group["time_berlin"].dt.year.isin([2021, 2023, 2024, 2025])
        ]

        rows_per_year = group_filtered.groupby(
            group_filtered["time_berlin"].dt.year
        ).size()

        counting_month = group_filtered.groupby(
            group_filtered["time_berlin"].dt.to_period("M")
        ).size()

        years_in_data = group_filtered["time_berlin"].dt.year.unique()

        valid_months = counting_month[counting_month >= min_rows_per_month].index

        group_valid = group_filtered[
            group_filtered["time_berlin"].dt.to_period("M").isin(valid_months)
        ]

        filtered_group = None

        # Single year - keep all

        if len(years_in_data) == 1:
            print(f"🥑 Malo {malo}: 1 year only, keeping all")
            filtered_group = group_valid

        # Multiple years with at least one full year

        elif len(years_in_data) > 2 and any(
            rows_per_year[rows_per_year.index.isin(years_in_data)] >= rows_per_full_year
        ):

            full_years = rows_per_year[
                rows_per_year >= rows_per_full_year
            ].index.tolist()

            print(f"🥑🥑 Malo {malo}: Full years available: {full_years}")

            filtered_group = group_valid[
                group_valid["time_berlin"].dt.year.isin(full_years)
            ]

        # Find continuous periods
        else:
            unique_months_sorted = sorted(
                group_valid["time_berlin"].dt.to_period("M").unique()
            )

            continuous_periods = []

            for period_length in [12, 24, 36, 48]:
                for i in range(len(unique_months_sorted) - period_length + 1):
                    start_month = unique_months_sorted[i]
                    if all(
                        (start_month + j) in unique_months_sorted
                        for j in range(period_length)
                    ):

                        continuous_periods.append(
                            [start_month + j for j in range(period_length)]
                        )

            if continuous_periods:
                most_recent = max(continuous_periods, key=lambda x: x[0])
                print(
                    f"🥑🥑🥑 Malo {malo}: Using 12/24/36/48 continuous period {most_recent[0]} to {most_recent[-1]}"
                )

                filtered_group = group_valid[
                    group_valid["time_berlin"].dt.to_period("M").isin(most_recent)
                ]

            else:
                print(f"⚠️ Malo {malo}: No continuous periods, keeping all valid months")
                filtered_group = group_valid

        if filtered_group is not None and not filtered_group.empty:
            available_years = filtered_group["time_berlin"].dt.year.unique().tolist()
            filtered_group = filtered_group.copy()
            filtered_group["available_years"] = ", ".join(map(str, available_years))
            filtered_data.append(filtered_group)

    if filtered_data:
        df_result = pd.concat(filtered_data, ignore_index=True)

        # Add available_months column
        df_result["month"] = df_result["time_berlin"].dt.to_period("M")

        month_counts = (
            df_result.groupby("malo")["month"]
            .nunique()
            .reset_index(name="available_months")
        )

        df_result = df_result.merge(month_counts, on="malo", how="left")

        df_result.drop(columns="month", inplace=True)

        print(
            f"✅ Filtered production data: {len(df_result)} rows, {df_result['malo'].nunique()} malos"
        )

        return df_result

    else:
        print("❌ No data remained after filtering")
        return pd.DataFrame()


# =============================================================================

# EXCEL OUTPUT

# =============================================================================


def save_multisheet_excel(df: pd.DataFrame, path: str, max_rows: int = EXCEL_MAX_ROWS):
    """Save large DataFrame to Excel with multiple sheets if needed"""

    with pd.ExcelWriter(path, engine="openpyxl") as writer:

        for i in range(0, len(df), max_rows):
            chunk = df.iloc[i : i + max_rows]
            sheet_name = f"Sheet_{i//max_rows + 1}"
            chunk.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"✅ Saved to {path}")


def format_excel_output(file_path: str):
    """Apply formatting to Excel output: header highlighting, column widths"""

    wb = load_workbook(file_path)

    highlight_fill = PatternFill(
        start_color="003366", end_color="003366", fill_type="solid"
    )

    white_font = Font(color="FFFFFF")

    for sheet_name in wb.sheetnames:

        sheet = wb[sheet_name]

        # Highlight header

        for cell in sheet[1]:

            cell.fill = highlight_fill

            cell.font = white_font

        # Auto-width columns

        for col in sheet.columns:

            max_length = 0

            column = col[0].column_letter

            for cell in col:

                try:

                    if len(str(cell.value)) > max_length:

                        max_length = len(cell.value)

                except BaseException:

                    pass

            adjusted_width = max_length + 1

            sheet.column_dimensions[column].width = adjusted_width

    wb.save(file_path)

    print(f"✅ Formatted Excel: {file_path}")


# =============================================================================

# BLINDLEISTER DATA PROCESSING

# =============================================================================


def process_blindleister_market_prices(
    df_flat: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """

    Process Blindleister market prices to calculate weighted deltas
    Returns:

        Tuple of (yearly_pivot, weighted_average_per_malo)

    """

    # Calculate spot RMV delta blindleister

    df_flat["spot_rmv_EUR_monthly_ytd"] = (
        df_flat["monthly_generated_energy_mwh"]
        * df_flat["monthly_market_price_eur_mwh"]
    ) - (
        df_flat["monthly_generated_energy_mwh"]
        * df_flat["monthly_reference_market_price_eur_mwh"]
    )

    # Aggregate per malo per year

    permalo_yearly = (
        df_flat.groupby(["year", "unit_mastr_id"], dropna=False)
        .agg(
            spot_rmv_EUR_yearly=("spot_rmv_EUR_monthly_ytd", "sum"),
            sum_prod_yearly=("monthly_generated_energy_mwh", "sum"),
        )
        .assign(blind_yearly=lambda x: x["spot_rmv_EUR_yearly"] / x["sum_prod_yearly"])
        .reset_index()
    )

    # Aggregate across all years per malo

    permalo_total = (
        df_flat.groupby("unit_mastr_id", dropna=False)
        .agg(
            spot_rmv_EUR_ytd=("spot_rmv_EUR_monthly_ytd", "sum"),
            sum_prod_ytd=("monthly_generated_energy_mwh", "sum"),
        )
        .assign(
            average_weighted_eur_mwh_blindleister=lambda x: x["spot_rmv_EUR_ytd"]
            / x["sum_prod_ytd"]
        )
        .reset_index()
    )

    # Pivot yearly data

    weighted_years_pivot = permalo_yearly.pivot(
        index="unit_mastr_id", columns="year", values="blind_yearly"
    ).reset_index()

    weighted_years_pivot.columns.name = None

    weighted_years_pivot = weighted_years_pivot.rename(
        columns={
            2021: "weighted_2021_eur_mwh_blindleister",
            2023: "weighted_2023_eur_mwh_blindleister",
            2024: "weighted_2024_eur_mwh_blindleister",
        }
    )

    # Round values

    cols_to_round = [
        "weighted_2021_eur_mwh_blindleister",
        "weighted_2023_eur_mwh_blindleister",
        "weighted_2024_eur_mwh_blindleister",
        "average_weighted_eur_mwh_blindleister",
    ]

    final_weighted = weighted_years_pivot.merge(
        permalo_total[["unit_mastr_id", "average_weighted_eur_mwh_blindleister"]],
        on="unit_mastr_id",
        how="left",
    )

    final_weighted[cols_to_round] = final_weighted[cols_to_round].round(2)

    print(f"✅ Processed Blindleister data for {len(final_weighted)} units")

    return final_weighted


def process_enervis_results(
    dfs: List[pd.DataFrame], target_years: List[str] = ["2021", "2023", "2024"]
) -> pd.DataFrame:
    """

    Process Enervis API results to create pivot table with yearly averages
    Args:

        dfs: List of DataFrames from Enervis API

        target_years: Years to include in pivot
    Returns:

        DataFrame with columns: id, 2021, 2023, 2024, avg_enervis

    """

    if not dfs:

        return pd.DataFrame()

    all_df = pd.concat(dfs, ignore_index=True)

    all_df["Year"] = all_df["Year"].astype(str)

    existing_years = all_df["Year"].unique().tolist()

    valid_years = [y for y in target_years if y in existing_years]

    if not valid_years:

        print("⚠️ No target years found in Enervis data")

        return pd.DataFrame()

    # Filter to valid years

    all_df = all_df[all_df["Year"].isin(valid_years)].copy()

    # Keep minimum Marktwertdifferenz per (id, Year)

    df_filtered = all_df.loc[
        all_df.groupby(["id", "Year"])["Marktwertdifferenz"].idxmin()
    ].copy()

    df_filtered["Marktwertdifferenz"] = df_filtered["Marktwertdifferenz"].round(2)

    # Pivot to wide format

    df_pivot = (
        df_filtered.pivot(index="id", columns="Year", values="Marktwertdifferenz")
        .rename_axis(None, axis=1)
        .reset_index()
    )

    # Ensure all year columns exist

    for year in target_years:

        if year not in df_pivot.columns:

            df_pivot[year] = np.nan

    # Calculate average

    df_pivot["avg_enervis"] = df_pivot[target_years].mean(axis=1, skipna=True).round(2)

    columns_to_keep = ["id"] + target_years + ["avg_enervis"]

    df_result = df_pivot[columns_to_keep]

    print(f"✅ Processed Enervis data for {len(df_result)} turbines")

    return df_result


# =============================================================================

# AGGREGATION FUNCTIONS

# =============================================================================


def aggregate_stammdaten_by_malo(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate stammdaten data by malo"""

    df["Power in MW"] = df["net_power_kw_unit"] / 1000

    agg_dict = {
        "unit_mastr_id": "first",
        "Projekt": "first",
        "tech": "first",
        "Power in MW": "sum",
        "INB": lambda x: [convert_date_or_keep_string(date) for date in x],
        "EEG": lambda x: list(x.unique()),
        "AW in EUR/MWh": lambda x: [
            round(float(val), 2) for val in x if is_number(val)
        ],
        "Curtailment & redispatch included": "first",
        "Balancing Cost": "first",
        "Curtailment_value_weighted": "first",
        "Trading Convenience": "first",
    }

    # Add Blindleister columns if they exist

    blindleister_cols = [
        "weighted_2021_eur_mwh_blindleister",
        "weighted_2023_eur_mwh_blindleister",
        "weighted_2024_eur_mwh_blindleister",
        "average_weighted_eur_mwh_blindleister",
    ]

    for col in blindleister_cols:

        if col in df.columns:

            agg_dict[col] = "min"

    df_agg = df.groupby(["malo"], dropna=False).agg(agg_dict).reset_index()

    print(f"✅ Aggregated {len(df_agg)} malos")

    return df_agg


# =============================================================================

# PRODUCTION METRICS + CURTAILMENT FORECASTING (ML)

# =============================================================================


def process_production_data(
    merge_prod_rmv_dayahead: pd.DataFrame, folder: Any
) -> Dict[str, pd.DataFrame]:
    """Compute monthly aggregation, weighted delta per malo, and capacity inputs."""

    df = merge_prod_rmv_dayahead.copy()

    #folder_path = Path(folder)

    # input_df_name = "merge_prod_rmv_dayahead"
    # folder_name = folder_path / f"{input_df_name}_forecast_output"
    # folder_name.mkdir(parents=True, exist_ok=True)

    df.rename(columns={"power_kwh": "production_kwh"}, inplace=True)

    if "year" not in df.columns and "time_berlin" in df.columns:

        df["year"] = pd.to_datetime(df["time_berlin"], errors="coerce").dt.year

    if "month" not in df.columns and "time_berlin" in df.columns:

        df["month"] = pd.to_datetime(df["time_berlin"], errors="coerce").dt.month

    df_dropdup = df.drop_duplicates(subset=["malo", "time_berlin", "production_kwh"])

    df_dropdup["deltaspot_eur"] = (
        df_dropdup["production_kwh"] * df_dropdup["dayaheadprice"] / 1000
    ) - (
        df_dropdup["production_kwh"]
        * df_dropdup["monthly_reference_market_price_eur_mwh"]
        / 1000
    )

    # try:
    #     df_dropdup.sort_values("time_berlin").to_excel(
    #         folder_path / "merge_prod_rmv_dayahead_forecasted.xlsx",
    #         index=False,
    #     )

    # except Exception:
    #     pass

    monthly_agg = (
        df_dropdup.groupby(["year", "month", "malo"])
        .agg(
            deltaspot_eur_monthly=("deltaspot_eur", "sum"),
            available_months=("available_months", "first"),
            available_years=("available_years", "first"),
        )
        .reset_index()
    )

    weighted_delta_permalo = (
        df_dropdup.groupby(["malo"])
        .agg(
            total_prod_kwh_malo=("production_kwh", "sum"),
            spot_rmv_eur_malo=("deltaspot_eur", "sum"),
        )
        .reset_index()
    )

    weighted_delta_permalo["weighted_delta_permalo"] = (
        weighted_delta_permalo["spot_rmv_eur_malo"]
        / (weighted_delta_permalo["total_prod_kwh_malo"] / 1000)
    ).round(2)

    for d in [df_dropdup, monthly_agg, weighted_delta_permalo]:

        d["malo"] = d["malo"].astype(str).str.strip()

    total_prod = df_dropdup.groupby(["malo"])["production_kwh"].sum()

    monthly_agg["total_prod_kwh"] = monthly_agg["malo"].map(total_prod)

    monthly_agg["total_prod_mwh"] = monthly_agg["total_prod_kwh"] / 1000

    year_agg = (
        monthly_agg.groupby(["malo"], dropna=False)
        .agg(
            available_months=("available_months", "first"),
            available_years=("available_years", "first"),
            total_prod_mwh=("total_prod_mwh", "first"),
        )
        .reset_index()
    )

    return {
        "monthly_agg": monthly_agg,
        "weighted_delta_permalo": weighted_delta_permalo,
        "year_agg": year_agg,
    }


def feature_engineering_classification(
    df: pd.DataFrame,
    feature_names: List[str],
) -> Tuple[pd.DataFrame, List[str], bool]:
    """Feature engineering for classification – must match training."""

    df = df.copy()

    if "volume__mw_imbalance" in df.columns:

        df["volume__mw_imbalance"] = pd.to_numeric(
            df["volume__mw_imbalance"], errors="coerce"
        ).fillna(0)

    else:

        df["volume__mw_imbalance"] = 0.0

    if "curtailment_kWh_per_kw" in df.columns:

        df["curtailment_flag"] = (df["curtailment_kWh_per_kw"] > 0).astype(int)

        has_actual_values = True

    else:

        has_actual_values = False

    df = df.rename(columns={"dayaheadprice": "dayaheadprice_eur_mwh"})

    for col in ["dayaheadprice_eur_mwh", "rebap_euro_per_mwh"]:

        if col in df.columns:

            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["quarterly_energy_kWh_per_kw"] = df["power_kwh"] / df["net_power_kw_unit"]

    df["DA_negative_flag"] = (df["dayaheadprice_eur_mwh"] < 0).astype(int)

    df["DA_negative_flag_lag_1"] = df["DA_negative_flag"].shift(1)

    df["rebap_negative_flag"] = (df["rebap_euro_per_mwh"] < 0).astype(int)

    df["rebap_negative_flag_lag_1"] = df["rebap_negative_flag"].shift(1)

    available_features = [f for f in feature_names if f in df.columns]

    missing_features = [f for f in feature_names if f not in df.columns]

    if missing_features:

        print(f"⚠️ Missing classification features: {missing_features}")

        if not available_features:

            raise ValueError("No required classification features available.")

    df_clean = df.dropna(subset=available_features).copy()

    if df_clean.empty:

        raise ValueError(
            "No valid rows after classification cleaning (NaNs in features)."
        )

    for f in available_features:

        df_clean[f] = pd.to_numeric(df_clean[f], errors="coerce")

    return df_clean, available_features, has_actual_values


def predict_curtailment_classification(
    df_new_prediction: pd.DataFrame,
    model_path: str,
    metadata_path: str,
    threshold_path: str,
    plot: bool = False,
) -> Optional[Dict[str, Any]]:
    """Run classification model on new data."""

    print_header("CLASSIFICATION – LOADING MODEL & METADATA")

    try:

        best_model = joblib.load(model_path)

        _ = metadata_path  # kept for compatibility

        with open(threshold_path, "r") as f:

            threshold_info = json.load(f)

    except FileNotFoundError as e:

        print(f"❌ Error loading classification files: {e}")

        return None

    feature_names = threshold_info["feature_names"]

    average_optimal_threshold = threshold_info["average_optimal_threshold"]

    print(f"Using optimal threshold: {average_optimal_threshold:.4f}")

    print_header("CLASSIFICATION – FEATURE ENGINEERING")

    df_clean, available_features, has_actual_values = (
        feature_engineering_classification(
            df_new_prediction,
            feature_names,
        )
    )

    X_new = df_clean[available_features]

    print(f"Classification rows: {len(X_new)}, features used: {available_features}")

    print_header("CLASSIFICATION – PREDICTION")

    y_proba = best_model.predict_proba(X_new)[:, 1]

    y_pred = (y_proba >= average_optimal_threshold).astype(int)

    df_clean["predicted_curtailment_probability"] = y_proba

    df_clean["predicted_curtailment_flag"] = y_pred

    df_clean["prediction_timestamp_cls"] = pd.Timestamp.now()

    print(
        f"Predicted curtailment == 1 for {y_pred.sum():,} rows "
        f"({y_pred.mean()*100:.1f}% of classified rows)."
    )

    if has_actual_values and "curtailment_flag" in df_clean.columns:

        y_actual = df_clean["curtailment_flag"]

        accuracy = accuracy_score(y_actual, y_pred)

        precision = precision_score(y_actual, y_pred, zero_division=0)

        recall = recall_score(y_actual, y_pred, zero_division=0)

        f1 = f1_score(y_actual, y_pred, zero_division=0)

        roc_auc = roc_auc_score(y_actual, y_proba)

        avg_precision = average_precision_score(y_actual, y_proba)

        print_header("CLASSIFICATION – METRICS (ACTUALS AVAILABLE)")
        print(f"Accuracy:      {accuracy:.4f}")
        print(f"Precision:     {precision:.4f}")
        print(f"Recall:        {recall:.4f}")
        print(f"F1-Score:      {f1:.4f}")
        print(f"ROC AUC:       {roc_auc:.4f}")
        print(f"Avg Precision: {avg_precision:.4f}")

    else:
        accuracy = precision = recall = f1 = roc_auc = avg_precision = None
        print("ℹ️ No actual curtailment available for classification metrics.")

    if plot:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
        sns.histplot(y_proba, bins=30, kde=True, ax=ax1)
        ax1.axvline(
            average_optimal_threshold, color="red", linestyle="--", label="Threshold"
        )
        ax1.set_title("Linear Scale")
        ax1.set_xlabel("P(curtailment=1)")
        ax1.set_ylabel("Frequency")
        ax1.legend()
        sns.histplot(y_proba, bins=30, kde=True, ax=ax2)

        ax2.axvline(
            average_optimal_threshold, color="red", linestyle="--", label="Threshold"
        )
        ax2.set_yscale("log")
        ax2.set_title("Log Scale")
        ax2.set_xlabel("P(curtailment=1)")
        ax2.set_ylabel("Frequency (log scale)")
        ax2.legend()

        plt.suptitle("Predicted Probability Distribution", fontsize=14)
        plt.tight_layout()
        plt.show()

    return {
        "predictions": df_clean,
        "model": best_model,
        "features_used": available_features,
        "optimal_threshold": average_optimal_threshold,
        "prediction_metrics": {
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "roc_auc": roc_auc,
            "avg_precision": avg_precision,
        },
        "prediction_stats": {
            "positive_predictions": int(y_pred.sum()),
            "negative_predictions": int(len(y_pred) - y_pred.sum()),
            "positive_rate": float(y_pred.mean()),
            "mean_probability": float(y_proba.mean()),
            "std_probability": float(y_proba.std()),
            "total_predictions": int(len(y_pred)),
        },
    }


def feature_engineering_regression(
    df: pd.DataFrame,
    reg_features: List[str],
) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    """Feature engineering for regression – must match training."""

    df = df.copy()

    exo_features = [
        "quarterly_energy_kWh_per_kw",
        "enwex_percentage",
        "dayaheadprice_eur_mwh",
        "rebap_euro_per_mwh",
        "volume__mw_imbalance",
        "id500_eur_mwh",
    ]

    for col in exo_features:

        if col in df.columns:

            df[col] = pd.to_numeric(df[col], errors="coerce").ffill().bfill()

        else:

            print(f"⚠️ Regression: missing feature {col} – filled with 0.")

            df[col] = 0.0

    if "curtailment_kWh_per_kw" in df.columns:

        df["curt_lag_1"] = df["curtailment_kWh_per_kw"].shift(1)

        df["curt_lag_2"] = df["curtailment_kWh_per_kw"].shift(2)

    available_features = [f for f in reg_features if f in df.columns]

    missing_features = [f for f in reg_features if f not in df.columns]

    if missing_features:

        print(f"⚠️ Missing regression features: {missing_features}")

        if not available_features:

            raise ValueError("No required regression features available.")

    df_clean = df.dropna(subset=available_features).copy()

    if df_clean.empty:

        raise ValueError("No valid rows after regression cleaning (NaNs in features).")

    X_new = df_clean[available_features].apply(pd.to_numeric, errors="coerce")

    return df_clean, X_new, available_features


def plot_regression_predictions(df_clean: pd.DataFrame):
    """Plot regression prediction distribution + time plot."""

    y_pred = df_clean["predicted_curtailment_kWh_per_kw"].values

    has_actual = "curtailment_kWh_per_kw" in df_clean.columns

    y_actual = df_clean["curtailment_kWh_per_kw"].values if has_actual else None

    if has_actual:

        y_actual_plot = df_clean[df_clean["curtailment_kWh_per_kw"] > 0][
            "curtailment_kWh_per_kw"
        ].values

        fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    else:

        fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    if has_actual:

        axes[0].hist(
            y_actual_plot,
            bins=30,
            alpha=0.6,
            color="blue",
            edgecolor="black",
            label="Actual",
        )

    axes[0].hist(
        y_pred,
        bins=30,
        alpha=0.6 if has_actual else 0.7,
        color="green",
        edgecolor="black",
        label="Predicted",
    )

    axes[0].set_xlabel("Curtailment (kWh/kW)")

    axes[0].set_ylabel("Frequency")

    axes[0].set_title("Curtailment Distribution")

    axes[0].grid(True, alpha=0.3)

    if has_actual:

        axes[0].legend()

    time_col = None

    for candidate in ["delivery_start_berlin", "time_berlin", "timestamp"]:

        if candidate in df_clean.columns:

            time_col = candidate

            break

    if time_col:

        df_sorted = df_clean.sort_values(time_col)

        x_vals = df_sorted[time_col]

        y_pred_sorted = df_sorted["predicted_curtailment_kWh_per_kw"]

        axes[1].plot(x_vals, y_pred_sorted, color="red", linewidth=1, label="Predicted")

        if has_actual:

            axes[1].plot(
                x_vals,
                df_sorted["curtailment_kWh_per_kw"],
                color="blue",
                linewidth=0.8,
                alpha=0.4,
                label="Actual",
            )

        axes[1].set_xlabel("Time")

        axes[1].tick_params(axis="x", rotation=45)

    else:

        x_vals = np.arange(len(y_pred))

        axes[1].plot(x_vals, y_pred, color="red", linewidth=1.2, label="Predicted")

        if has_actual and y_actual is not None:

            axes[1].plot(
                x_vals, y_actual, color="blue", linewidth=1.2, alpha=0.8, label="Actual"
            )

        axes[1].set_xlabel("Sample Index")

    axes[1].set_ylabel("Curtailment (kWh/kW)")

    axes[1].set_title("Curtailment Over Time")

    if has_actual:

        axes[1].legend()

    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()

    plt.show()


def predict_curtailment_regression(
    df_reg_input: pd.DataFrame,
    model_path: str,
    params_path: str,
    plot: bool = False,
) -> Optional[Dict[str, Any]]:
    """Run regression model on subset of rows (already filtered by classification)."""

    try:

        best_model = joblib.load(model_path)

        with open(params_path, "r") as f:

            best_params = json.load(f)

        _ = best_params

    except FileNotFoundError as e:

        print(f"❌ Error loading regression files: {e}")

        return None

    reg_features = [
        "quarterly_energy_kWh_per_kw",
        "enwex_percentage",
        "dayaheadprice_eur_mwh",
        "rebap_euro_per_mwh",
        "volume__mw_imbalance",
        "id500_eur_mwh",
    ]

    if df_reg_input.empty:

        print("ℹ️ No rows passed to regression (no predicted curtailment = 1).")

        return {
            "predictions": df_reg_input.assign(predicted_curtailment_kWh_per_kw=np.nan),
            "model": best_model,
            "features_used": reg_features,
            "prediction_metrics": {"mse": None, "mae": None, "mape": None, "r2": None},
            "prediction_stats": {
                "mean": None,
                "std": None,
                "min": None,
                "max": None,
                "count": 0,
            },
        }

    print_header("REGRESSION – FEATURE ENGINEERING ON FILTERED ROWS")

    df_clean, X_new, used_features = feature_engineering_regression(
        df_reg_input, reg_features
    )

    print(f"Regression rows: {len(X_new)}, features used: {used_features}")

    y_pred = best_model.predict(X_new)

    df_clean["predicted_curtailment_kWh_per_kw"] = y_pred

    df_clean["prediction_timestamp_reg"] = pd.Timestamp.now()

    if "curtailment_kWh_per_kw" in df_clean.columns:

        y_actual = df_clean["curtailment_kWh_per_kw"]

        mse = mean_squared_error(y_actual, y_pred)

        mae = mean_absolute_error(y_actual, y_pred)

        mape = mean_absolute_percentage_error(y_actual, y_pred)

        r2 = r2_score(y_actual, y_pred)

        print_header("REGRESSION – METRICS (ACTUALS AVAILABLE)")

        print(f"MSE:  {mse:.4f}")

        print(f"MAE:  {mae:.4f}")

        print(f"MAPE: {mape:.4f}")

        print(f"R²:   {r2:.4f}")

    else:

        mse = mae = mape = r2 = None

        print("ℹ️ No actual curtailment available for regression metrics.")

    if plot:

        plot_regression_predictions(df_clean)

    return {
        "predictions": df_clean,
        "model": best_model,
        "features_used": used_features,
        "prediction_metrics": {"mse": mse, "mae": mae, "mape": mape, "r2": r2},
        "prediction_stats": {
            "mean": float(np.mean(y_pred)),
            "std": float(np.std(y_pred)),
            "min": float(np.min(y_pred)),
            "max": float(np.max(y_pred)),
            "count": int(len(y_pred)),
        },
    }


def run_curtailment_forecast(
    df_new_prediction: pd.DataFrame,
    cls_model_path: str,
    cls_meta_path: str,
    cls_thresh_path: str,
    reg_model_path: str,
    reg_params_path: str,
    plot_class: bool = False,
    plot_reg: bool = False,
) -> Optional[Dict[str, Any]]:
    """Classification on all rows, regression on predicted-curtailment subset, then merge back."""

    cls_results = predict_curtailment_classification(
        df_new_prediction,
        model_path=cls_model_path,
        metadata_path=cls_meta_path,
        threshold_path=cls_thresh_path,
        plot=plot_class,
    )

    if cls_results is None:

        return None

    df_cls = cls_results["predictions"].copy()

    if "predicted_curtailment_flag" not in df_cls.columns:

        print("❌ Classification result missing 'predicted_curtailment_flag'.")

        return {"classification": cls_results, "regression": None, "combined": df_cls}

    df_for_reg = df_cls[df_cls["predicted_curtailment_flag"] == 1].copy()

    print_header("PIPELINE – ROWS FOR REGRESSION")

    print(f"Rows flagged as curtailment (1): {len(df_for_reg)}")

    reg_results = predict_curtailment_regression(
        df_for_reg,
        model_path=reg_model_path,
        params_path=reg_params_path,
        plot=plot_reg,
    )

    df_combined = df_cls.copy()

    df_combined["predicted_curtailment_kWh_per_kw"] = np.nan

    if reg_results is not None and not reg_results["predictions"].empty:

        df_reg_pred = reg_results["predictions"].copy()

        merge_keys = ["malo", "delivery_start_berlin"]

        df_reg_pred = df_reg_pred[merge_keys + ["predicted_curtailment_kWh_per_kw"]]

        df_combined = df_combined.merge(
            df_reg_pred,
            on=merge_keys,
            how="left",
            suffixes=("", "_reg"),
        )

        df_combined["predicted_curtailment_kWh_per_kw"] = df_combined[
            "predicted_curtailment_kWh_per_kw_reg"
        ]

        df_combined.drop(columns=["predicted_curtailment_kWh_per_kw_reg"], inplace=True)

        df_combined["predicted_curtailment_kWh_per_kw"] = pd.to_numeric(
            df_combined["predicted_curtailment_kWh_per_kw"],
            errors="coerce",
        ).fillna(0)

    return {
        "classification": cls_results,
        "regression": reg_results,
        "combined": df_combined,
    }


def set_paths_for_category(category: str) -> Dict[str, str]:

    base_path = os.getenv("CURTAILMENT_MODEL_BASE_PATH", MODEL_BASE_PATH)

    category_mapping = {
        "PV_rules": "PV_rules",
        "PV_no_rules": "PV_NORULES",
        "WIND_rules": "WIND_rules",
        "WIND_no_rules": "WIND_NORULES",
    }

    if category not in category_mapping:

        raise ValueError(f"Unknown category: {category}")

    folder_name = category_mapping[category]

    return {
        "CLASS_MODEL_PATH": f"{base_path}/{folder_name}/classification_best_model_{folder_name}.joblib",
        "CLASS_META_PATH": f"{base_path}/{folder_name}/classification_xgboost_curtailment_model_{folder_name}.joblib",
        "CLASS_THRESH_PATH": f"{base_path}/{folder_name}/classification_best_params_{folder_name}.json",
        "REG_MODEL_PATH": f"{base_path}/{folder_name}/regression_best_model_{folder_name}.joblib",
        "REG_PARAMS_PATH": f"{base_path}/{folder_name}/regression_best_params_{folder_name}.json",
    }


def run_curtailment_forecast_multi_category(
    df_ts: pd.DataFrame,
    plot_class: bool = False,
    plot_reg: bool = False,
) -> Optional[Dict[str, Any]]:
    """Run the full classification->regression pipeline for each EEG category."""

    df_ts = df_ts.copy()

    if "category" not in df_ts.columns:

        if "tech" in df_ts.columns and "EEG" in df_ts.columns:

            df_ts["category"] = np.where(
                (df_ts["tech"] == "PV") & (df_ts["EEG"] == "no rules"),
                "PV_no_rules",
                np.where(
                    df_ts["tech"] == "PV",
                    "PV_rules",
                    np.where(
                        (df_ts["tech"] == "WIND") & (df_ts["EEG"] == "no rules"),
                        "WIND_no_rules",
                        "WIND_rules",
                    ),
                ),
            )

        else:

            raise ValueError(
                "df_ts must contain 'category' (or at least 'tech' and 'EEG' to derive it)."
            )

    if df_ts["category"].isna().any():
        missing_malo = df_ts.loc[df_ts["category"].isna(), "malo"].unique()
        raise ValueError(
            f"Some malos in time series do not have a category: {missing_malo}"
        )

    all_combined: List[pd.DataFrame] = []
    all_results_by_category: Dict[str, Any] = {}

    for category, df_cat in df_ts.groupby("category"):

        print(df_ts["category"].unique().tolist())
        print_header(f"🍫🍫 RUNNING CATEGORY: {category}")
        paths = set_paths_for_category(category)

        res = run_curtailment_forecast(
            df_new_prediction=df_cat,
            cls_model_path=paths["CLASS_MODEL_PATH"],
            cls_meta_path=paths["CLASS_META_PATH"],
            cls_thresh_path=paths["CLASS_THRESH_PATH"],
            reg_model_path=paths["REG_MODEL_PATH"],
            reg_params_path=paths["REG_PARAMS_PATH"],
            plot_class=plot_class,
            plot_reg=plot_reg,
        )

        if res is not None:
            combined_cat = res["combined"].copy()
            combined_cat["category"] = category
            all_combined.append(combined_cat)
            all_results_by_category[category] = res

    if not all_combined:
        return None

    df_all = pd.concat(all_combined, ignore_index=True)

    return {"by_category": all_results_by_category, "combined": df_all}


# =============================================================================

# REPORT GENERATION

# =============================================================================


def generate_output_sheets(
    df: pd.DataFrame, has_production: bool = False, has_forecast: bool = False
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """

    Generate three output sheets for customer report
    Returns:

        Tuple of (sheet1, sheet2, sheet3)

    """

    # Define column orders for each sheet

    sheet1_order = [
        "malo",
        "Projekt",
        "Technology",
        "Power in MW",
        "INB",
        "EEG",
        "AW in EUR/MWh",
        "weighted_2021_eur_mwh_blindleister",
        "weighted_2023_eur_mwh_blindleister",
        "weighted_2024_eur_mwh_blindleister",
        "average_weighted_eur_mwh_blindleister",
        "2021",
        "2023",
        "2024",
        "avg_enervis",
        "weighted_delta_permalo",
        "forecast_weighted_delta_permalo" if has_forecast else None,
        "Curtailment & redispatch included",
        "available_months",
        "available_years",
        "capacity_factor_percent",
        "forecast_capacity_factor_percent" if has_forecast else None,
        "Balancing Cost",
        "Curtailment_value_weighted",
        "AMW/RWM Delta",
        "Fair Value",
        "Trading Convenience",
    ]

    sheet2_order = [
        "malo",
        "Projekt",
        "Technology",
        "Power in MW",
        "INB",
        "EEG",
        "AW in EUR/MWh",
        "average_weighted_eur_mwh_blindleister",
        "avg_enervis",
        "weighted_delta_permalo",
        "forecast_weighted_delta_permalo" if has_forecast else None,
        "Curtailment & redispatch included",
        "available_months",
        "available_years",
        "capacity_factor_percent",
        "Balancing Cost",
        "Curtailment_value_weighted",
        "AMW/RWM Delta",
        "Fair Value",
        "Trading Convenience",
        "Fee EUR/MWh",
    ]

    sheet3_order = [
        "malo",
        "Projekt",
        "Technology",
        "Power in MW",
        "AW in EUR/MWh",
        "Fee EUR/MWh",
    ]

    # Remove production-only columns if no production data

    if not has_production:
        for col in [
            "weighted_delta_permalo",
            "forecast_weighted_delta_permalo",
            "forecast_capacity_factor_percent",
            "available_months",
            "available_years",
            "capacity_factor_percent",
        ]:

            if col in sheet1_order:
                sheet1_order.remove(col)

            if col in sheet2_order:
                sheet2_order.remove(col)

    sheet1 = ensure_and_reorder(df.copy(), sheet1_order)
    sheet2 = ensure_and_reorder(df.copy(), sheet2_order)
    sheet3 = ensure_and_reorder(df.copy(), sheet3_order)

    return sheet1, sheet2, sheet3


# =============================================================================

# MAIN EXECUTION

# =============================================================================


def main():
    """
    Main execution pipeline handling all scenarios:
    Scenarios handled:

    1. SEE data available → Fetch Blindleister market prices
    2. SEE wind turbines → Call Enervis API for market value differentials
    3. Non-SEE wind turbines with manufacturer/model → Match and call Enervis
    4. Merge SEE and non-SEE Enervis results
    5. Production data available → Process time series, curtailment, redispatch
    6. Historical data 2023-2025 → Run ML curtailment forecasting
    7. Generate reports with all available data
    """

    # folder = Path(
    #     r"C:\Users\JerrySetiawan\OneDrive - CFP FlexPower\Work\Data pricing\DWAG\DWAG_curt_testing_v2"
    # )

    # folder_name = folder.name
    # path = folder / f"{folder_name}_stammdaten.xlsx"
    # out_path = folder / f"{folder_name}_customerpricing.xlsx"
    # out_path_highlighted = folder / f"{folder_name}_customerpricing_highlighted.xlsx"

    #folder = Path(__file__).parent.resolve()
    folder = Path(r"C:\Users\JerrySetiawan\OneDrive - CFP FlexPower\Work\Data pricing\DWAG\DWAG_curt_testing_v2")
    folder_name = folder.name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    timestamp_sec = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Files with timestamp to prevent overwrites
    path = folder / f"{folder_name}_stammdaten.xlsx"
    out_path = folder / f"{folder_name}_customerpricing_{timestamp}.xlsx"
    nan_path = folder / f"df_turbines_with_match_nan_matched_turbine_id_with_see_{timestamp_sec}.xlsx"
    #out_path_highlighted = folder / f"{folder_name}_customerpricing_highlighted_{timestamp}.xlsx"

    print(f"✅ Script folder: {folder}")
    if path.exists():
        print(f"✅ Found input: {path.name}")
    else:
        print(f"⚠️  Input file not found: {path.name}")
    print_header("STARTING ENERGY PRICING PIPELINE")

    # =========================================================================

    # STEP 1: Load master data and apply fixings

    # =========================================================================

    print_header("STEP 1: LOADING DATA")

    df_fixings = load_bigquery_fixings(PROJECT_ID)
    df_stamm = load_stammdaten(path)

    print_header("ASSIGNING EEG CATEGORIES")
    df_stamm = set_category_based_on_conditions(df_stamm)

    print_header("APPLYING FIXINGS")
    fixings = extract_all_fixings(df_fixings, year="2026")
    df_stamm = apply_fixings_to_stammdaten(df_stamm, fixings)

    # Track what data we have
    df_assets_see_enriched = None
    df_assets_nonsee_enervis_enriched = None
    df_enervis_mv_by_malo = None

    # =========================================================================

    # STEP 2: Process SEE data (Blindleister + Enervis)

    # =========================================================================

    df_see_units = df_stamm[
        df_stamm["unit_mastr_id"].str.strip().str.lower().str.startswith("see")
    ].copy()

    if not df_see_units.empty:
        print_header("STEP 2A: PROCESSING SEE DATA - BLINDLEISTER")

        # Extract valid SEE IDs

        see_unit_ids = [
            str(id_).strip().upper()
            for id_ in df_see_units["unit_mastr_id"].dropna()
            if str(id_).strip().lower().startswith("see")
        ]

        print(f"Found {len(set(see_unit_ids))} unique SEE units")

        # Fetch Blindleister market prices

        blindleister = BlindleisterAPI(BLINDLEISTER_EMAIL, BLINDLEISTER_PASSWORD)

        df_blindleister_market_prices_raw = blindleister.get_market_prices(
            see_unit_ids, [2021, 2023, 2024]
        )

        if not df_blindleister_market_prices_raw.empty:

            # Process Blindleister data
            df_blindleister_weighted_mv = process_blindleister_market_prices(
                df_blindleister_market_prices_raw
            )

            # Merge with stammdaten
            df_assets_with_blindleister = pd.merge(
                df_stamm, df_blindleister_weighted_mv, on="unit_mastr_id", how="left"
            )

            # Convert malo to string
            df_assets_with_blindleister["malo"] = (
                df_assets_with_blindleister["malo"].astype(str).str.strip()
            )

            # Check if SEE data contains wind turbines
            df_see_wind_units = df_see_units[
                df_see_units["tech"].str.upper() == "WIND"
            ].copy()

            if not df_see_wind_units.empty:
                print_header("STEP 2B: PROCESSING SEE WIND - ENERVIS")

                # Load turbine reference
                df_enervis_turbine_reference = pd.read_excel(TURBINE_REFERENCE_PATH)

                df_enervis_turbine_reference["id"] = (
                    df_enervis_turbine_reference["id"].astype(str).str.strip()
                )

                # Prepare turbine data for matching
                df_turbines_with_match = prepare_turbine_matching_dataframe(
                    df_see_wind_units, df_enervis_turbine_reference, threshold=76, nan_path= nan_path
                )

                # Export unmatched turbines for review (Matched_Turbine_ID is
                # NaN)
                # df_unmatched_turbines = df_turbines_with_match[
                #     df_turbines_with_match["Matched_Turbine_ID"].isna()
                # ].copy()

                #df_turbines_with_match.to_excel(folder/"df_turbines_with_match_nan_matched_turbine_id_with_see.xlsx", index=False)

                # if not df_turbines_with_match.empty:
                #     save_multisheet_excel(
                #         df_turbines_with_match,
                #         str(
                #             folder
                #             / "df_turbines_with_match_nan_matched_turbine_id.xlsx"
                #         ),
                #     )

                # Filter valid turbines

                df_matched_turbines_for_enervis = df_turbines_with_match.dropna(
                    subset=["Matched_Turbine_ID"]
                ).copy()

                df_matched_turbines_for_enervis["hub_height_m"] = (
                    df_matched_turbines_for_enervis["hub_height_m"]
                    .fillna(104)
                    .replace(0, 104)
                )

                df_matched_turbines_for_enervis["hub_height_m"] = (
                    df_matched_turbines_for_enervis["hub_height_m"].astype(int)
                )

                df_matched_turbines_for_enervis["Matched_Turbine_ID"] = (
                    df_matched_turbines_for_enervis["Matched_Turbine_ID"].astype(int)
                )

                df_matched_turbines_for_enervis["malo"] = (
                    df_matched_turbines_for_enervis["malo"].astype(str).str.strip()
                )

                if not df_matched_turbines_for_enervis.empty:

                    # Call Enervis API

                    anemos = AnemosAPI(ANEMOS_EMAIL, ANEMOS_PASSWORD)

                    product_id = anemos.get_historical_product_id()

                    # Build parkinfo

                    parkinfo = []

                    for _, row in df_matched_turbines_for_enervis.iterrows():

                        parkinfo.append(
                            {
                                "id": int(row["malo"]),
                                "lat": str(row["latitude"]),
                                "lon": str(row["longitude"]),
                                "turbine_type_id": int(row["Matched_Turbine_ID"]),
                                "hub_height": int(row["hub_height_m"]),
                            }
                        )

                    job_uuid = anemos.start_job(product_id, parkinfo)

                    if job_uuid:

                        job_info = anemos.wait_for_job(job_uuid)

                        dfs = anemos.extract_results(job_info)

                        df_enervis_mv_by_malo = process_enervis_results(dfs)

            # Aggregate SEE data by malo

            df_assets_see_agg_by_malo = aggregate_stammdaten_by_malo(
                df_assets_with_blindleister
            )

            # Merge with Enervis if available

            if df_enervis_mv_by_malo is not None and not df_enervis_mv_by_malo.empty:

                df_enervis_mv_by_malo["id"] = df_enervis_mv_by_malo["id"].astype(str)

                df_assets_see_enriched = pd.merge(
                    df_assets_see_agg_by_malo,
                    df_enervis_mv_by_malo,
                    left_on="malo",
                    right_on="id",
                    how="left",
                )

                df_assets_see_enriched.drop(columns=["id"], inplace=True)

                df_assets_see_enriched = df_assets_see_enriched.rename(
                    columns={"tech": "Technology"}
                )

            else:

                df_assets_see_enriched = df_assets_see_agg_by_malo.rename(
                    columns={"tech": "Technology"}
                )

        else:

            print("⚠️ No Blindleister data fetched for SEE units")

    # =========================================================================

    # STEP 3: Process non-SEE wind turbines (manual stammdaten)

    # =========================================================================

    df_nonsee_units = df_stamm[
        pd.isna(df_stamm["unit_mastr_id"])
        | (df_stamm["unit_mastr_id"].str.strip() == "")
        | (df_stamm["unit_mastr_id"].str.lower() == "nan")
    ].copy()

    # Also add SEE units that didn't get Enervis results

    if df_assets_see_enriched is not None:

        malo_need_enervis = (
            df_assets_see_enriched[df_assets_see_enriched["avg_enervis"].isna()]["malo"]
            .astype(str)
            .str.strip()
            .unique()
        )

        if len(malo_need_enervis) > 0:

            stamm_for_empty = df_stamm[
                df_stamm["malo"].astype(str).str.strip().isin(malo_need_enervis)
            ].copy()

            df_nonsee_units = pd.concat(
                [df_nonsee_units, stamm_for_empty], ignore_index=True
            )

    if not df_nonsee_units.empty:

        df_nonsee_wind_candidates = df_nonsee_units[
            (df_nonsee_units["tech"].str.upper() == "WIND")
            & df_nonsee_units["manufacturer"].notna()
            & df_nonsee_units["turbine_model"].notna()
        ].copy()

        if not df_nonsee_wind_candidates.empty:
            print_header(
                "STEP 3: PROCESSING NON-SEE WIND - ENERVIS, INCLUDING NO RESULT"
            )

            # Load turbine reference
            df_enervis_turbine_reference = pd.read_excel(TURBINE_REFERENCE_PATH)

            df_enervis_turbine_reference["id"] = (
                df_enervis_turbine_reference["id"].astype(str).str.strip()
            )

            # Prepare turbine data

            df_nonsee_wind_candidates["net_power_kw"] = df_nonsee_wind_candidates[
                "net_power_kw_unit"
            ]

            df_turbines_with_match = prepare_turbine_matching_dataframe(
                df_nonsee_wind_candidates, df_enervis_turbine_reference, threshold=76, nan_path= nan_path
            )

            # Export unmatched turbines for review (Matched_Turbine_ID is NaN)

            # df_unmatched_turbines = df_turbines_with_match[
            #     df_turbines_with_match["Matched_Turbine_ID"].isna()
            # ].copy()

            #df_turbines_with_match.to_excel(folder/"df_turbines_with_match_nan_matched_turbine_id_no_see.xlsx", index=False)

            # if not df_unmatched_turbines.empty:

            #     save_multisheet_excel(
            #         df_unmatched_turbines,
            #         str(folder / "df_turbines_with_match_nan_matched_turbine_id.xlsx"),
            #     )

            # Filter valid turbines

            df_matched_turbines_for_enervis = df_turbines_with_match.dropna(
                subset=["Matched_Turbine_ID"]
            ).copy()

            df_matched_turbines_for_enervis["hub_height_m"] = (
                df_matched_turbines_for_enervis["hub_height_m"]
                .fillna(104)
                .replace(0, 104)
            )

            df_matched_turbines_for_enervis["hub_height_m"] = (
                df_matched_turbines_for_enervis["hub_height_m"].apply(
                    lambda x: max(int(x), 50)
                )
            )

            df_matched_turbines_for_enervis["Matched_Turbine_ID"] = (
                df_matched_turbines_for_enervis["Matched_Turbine_ID"].astype(int)
            )

            df_matched_turbines_for_enervis["malo"] = (
                df_matched_turbines_for_enervis["malo"].astype(str).str.strip()
            )

            if not df_matched_turbines_for_enervis.empty:

                # Call Enervis API
                anemos = AnemosAPI(ANEMOS_EMAIL, ANEMOS_PASSWORD)
                product_id = anemos.get_historical_product_id()
                # Build parkinfo
                parkinfo = []
                for _, row in df_matched_turbines_for_enervis.iterrows():
                    parkinfo.append(
                        {
                            "id": int(row["malo"]),
                            "lat": str(row["latitude"]),
                            "lon": str(row["longitude"]),
                            "turbine_type_id": int(row["Matched_Turbine_ID"]),
                            "hub_height": int(row["hub_height_m"]),
                        }
                    )

                job_uuid = anemos.start_job(product_id, parkinfo)

                if job_uuid:
                    job_info = anemos.wait_for_job(job_uuid)
                    dfs = anemos.extract_results(job_info)
                    df_enervis_mv_nonsee = process_enervis_results(dfs)

                    # Merge with stammdaten

                    if df_assets_see_enriched is not None:
                        # We have SEE data, aggregate non-SEE separately
                        df_assets_agg_by_malo = aggregate_stammdaten_by_malo(df_stamm)

                    else:
                        # No SEE data, use full stammdaten
                        df_assets_agg_by_malo = aggregate_stammdaten_by_malo(df_stamm)

                    df_enervis_mv_nonsee["id"] = df_enervis_mv_nonsee["id"].astype(str)

                    df_assets_nonsee_enervis_enriched = pd.merge(
                        df_assets_agg_by_malo,
                        df_enervis_mv_nonsee,
                        left_on="malo",
                        right_on="id",
                        how="left",
                    )

                    df_assets_nonsee_enervis_enriched.drop(columns=["id"], inplace=True)

                    df_assets_nonsee_enervis_enriched = (
                        df_assets_nonsee_enervis_enriched.dropna(
                            subset=["2021", "2023", "2024", "avg_enervis"]
                        )
                    )

                    df_assets_nonsee_enervis_enriched = (
                        df_assets_nonsee_enervis_enriched.rename(
                            columns={"tech": "Technology"}
                        )
                    )

    # =========================================================================

    # STEP 4: Merge SEE and non-SEE Enervis results

    # =========================================================================

    print_header("STEP 4: MERGING RESULTS")

    if (
        df_assets_see_enriched is not None
        and df_assets_nonsee_enervis_enriched is not None
    ):

        print("Merging SEE and non-SEE Enervis results")

        df_assets_enriched = pd.merge(
            df_assets_see_enriched,
            df_assets_nonsee_enervis_enriched[
                ["malo", "2021", "2023", "2024", "avg_enervis"]
            ],
            on="malo",
            how="left",
            suffixes=("_see", "_no_see"),
        )

        # Fill missing values

        for col in ["2021", "2023", "2024", "avg_enervis"]:

            df_assets_enriched[col] = df_assets_enriched[col + "_see"].fillna(
                df_assets_enriched[col + "_no_see"]
            )

            df_assets_enriched.drop(
                columns=[col + "_see", col + "_no_see"], inplace=True
            )

    elif df_assets_see_enriched is not None:

        print("Using SEE results only")

        df_assets_enriched = df_assets_see_enriched

    elif df_assets_nonsee_enervis_enriched is not None:

        print("Using non-SEE results only")

        df_assets_enriched = df_assets_nonsee_enervis_enriched

    else:

        print("No Enervis results, using stammdaten aggregation only")
        df_assets_enriched = aggregate_stammdaten_by_malo(df_stamm)
        df_assets_enriched = df_assets_enriched.rename(columns={"tech": "Technology"})

    # =========================================================================

    # STEP 5: Check for production data

    # =========================================================================

    xls = pd.ExcelFile(path, engine="openpyxl")

    sheet_names = xls.sheet_names
    has_production = len(sheet_names) > 1 and sheet_names[0].lower() == "stammdaten"

    if not has_production:

        print_header("STEP 5: NO PRODUCTION DATA - GENERATING SIMPLE REPORT")

        # Generate output sheets

        sheet1, sheet2, sheet3 = generate_output_sheets(
            df_assets_enriched, has_production=False, has_forecast=False
        )

        # Save to Excel

        customer_name = os.path.basename(os.path.dirname(out_path))
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            sheet1.to_excel(writer, sheet_name=f"{customer_name}_1", index=False)
            sheet2.to_excel(writer, sheet_name=f"{customer_name}_2", index=False)
            sheet3.to_excel(writer, sheet_name=f"{customer_name}_3", index=False)

        format_excel_output(out_path)

        print_header("PIPELINE COMPLETE")

        print(f"✅ Output saved to {out_path}")
        # print(f"✅ Formatted output saved to {out_path_highlighted}")

        return

    # =========================================================================

    # STEP 6: Process production data (time series)

    # =========================================================================

    print_header("STEP 6: PROCESSING PRODUCTION DATA")

    # Load production sheets

    sheets = [
        s
        for s in sheet_names
        if s.lower()
        not in [
            "stammdaten",
            "curtailment",
            "redispatch_wind",
            "redispatch_pv",
            "redispatch",
        ]
    ]

    batch_size = 5

    merged = []

    for i in range(0, len(sheets), batch_size):

        batch = sheets[i : i + batch_size]

        dfs = []

        for sh in batch:

            df_tmp = pd.read_excel(path, sheet_name=sh, engine="openpyxl")

            df_tmp.columns = df_tmp.columns.str.strip()

            if "malo" in df_tmp.columns:

                df_tmp["malo"] = df_tmp["malo"].astype(str).str.strip()

            dfs.append(df_tmp)

        merged.append(pd.concat(dfs, ignore_index=True))

        del dfs

        gc.collect()

    df_production_raw = pd.concat(merged, ignore_index=True)

    del merged

    gc.collect()

    df_production_raw.columns = df_production_raw.columns.str.strip()

    df_production_raw["malo"] = df_production_raw["malo"].astype(str).str.strip()

    # Process time column

    df_production_raw = process_time_column(df_production_raw)

    # Filter by completeness

    df_production_filtered = filter_production_data_by_completeness(df_production_raw)

    if df_production_filtered.empty:

        print("❌ No valid production data after filtering")

        return

    # Rename power column

    df_production_filtered.rename(columns={"power_kwh": "infeed_kwh"}, inplace=True)

    df_production_filtered["__adj_kwh"] = 0.0

    # Load curtailment/redispatch if available

    wb = load_workbook(path, read_only=True)

    available_sheets = wb.sheetnames

    wb.close()

    if "redispatch" in available_sheets:

        df_redispatch = pd.read_excel(path, sheet_name="redispatch", engine="openpyxl")

        df_redispatch.columns = df_redispatch.columns.str.strip()

        df_redispatch["malo"] = df_redispatch["malo"].astype(str).str.strip()

        df_redispatch = process_time_column(df_redispatch)

        df_production_filtered = pd.merge(
            df_production_filtered,
            df_redispatch[["malo", "time_berlin", "redispatch_kwh"]],
            on=["malo", "time_berlin"],
            how="left",
        )

        df_production_filtered["__adj_kwh"] = df_production_filtered[
            "__adj_kwh"
        ] + df_production_filtered["redispatch_kwh"].fillna(0)

    if "curtailment" in available_sheets:

        df_curtailment = pd.read_excel(
            path, sheet_name="curtailment", engine="openpyxl"
        )

        df_curtailment.columns = df_curtailment.columns.str.strip()

        df_curtailment["malo"] = df_curtailment["malo"].astype(str).str.strip()

        df_curtailment = process_time_column(df_curtailment)

        df_production_filtered = pd.merge(
            df_production_filtered,
            df_curtailment[["malo", "time_berlin", "curtailment_kwh"]],
            on=["malo", "time_berlin"],
            how="left",
        )

        df_production_filtered["curtailment_kwh"] = df_production_filtered[
            "curtailment_kwh"
        ].fillna(0)

        df_production_filtered["__adj_kwh"] = (
            df_production_filtered["__adj_kwh"]
            + df_production_filtered["curtailment_kwh"]
        )

        df_production_filtered["curtailment"] = df_production_filtered["malo"].isin(
            df_curtailment["malo"]
        )

    df_production_filtered["infeed_kwh"] = pd.to_numeric(
        df_production_filtered["infeed_kwh"], errors="coerce"
    )

    df_production_filtered["__adj_kwh"] = pd.to_numeric(
        df_production_filtered["__adj_kwh"], errors="coerce"
    )

    df_production_filtered["power_kwh"] = (
        df_production_filtered["infeed_kwh"] + df_production_filtered["__adj_kwh"]
    )

    # =========================================================================

    # STEP 7: Merge with price data and calculate deltas

    # =========================================================================

    print_header("STEP 7: MERGING WITH PRICE DATA")

    # Load day-ahead and RMV prices

    df_dayahead_prices = load_day_ahead_prices(DAY_AHEAD_PRICE_PATH)

    df_rmv_prices = load_rmv_prices(RMV_PRICE_PATH)

    # Expand hourly to quarter-hourly
    df_dayahead_prices_qh = expand_hourly_to_quarter_hourly(df_dayahead_prices)

    # Aggregate production
    grouped = df_production_filtered.groupby(
        ["malo", "time_berlin", "available_years", "available_months"]
    )

    def custom_power_mwh(group):

        if group.nunique() == 1:

            return group.mean()

        else:

            return group.sum()

    df_production_qh_agg = grouped["power_kwh"].apply(custom_power_mwh).reset_index()

    # Merge with assets mapping

    pattern = r"\d+.*rules"

    df_temp = df_stamm.copy()

    df_temp["_sort_priority"] = df_temp["category"].str.contains(pattern, na=False)

    df_sorted = df_temp.sort_values(["malo", "_sort_priority"], ascending=[True, False])

    df_assets_mapping = (
        df_sorted.groupby(["malo"], dropna=False)
        .agg(
            {
                "tech": "first",
                "net_power_kw_unit": "sum",
                "category": "first",
            }
        )
        .reset_index()
    )

    df_production_qh_agg = df_production_qh_agg.merge(
        df_assets_mapping, on="malo", how="left"
    )

    # Merge with day-ahead prices

    df_prod_with_dayahead = pd.merge(
        df_production_qh_agg, df_dayahead_prices_qh, on="time_berlin", how="inner"
    )

    # Add year/month

    df_prod_with_dayahead["year"] = df_prod_with_dayahead["time_berlin"].dt.year.astype(
        "int16"
    )

    df_prod_with_dayahead["month"] = df_prod_with_dayahead[
        "time_berlin"
    ].dt.month.astype("int8")

    df_prod_with_dayahead["tech"] = (
        df_prod_with_dayahead["tech"].str.strip().str.upper().astype("category")
    )

    # Merge with RMV

    df_prod_with_prices = df_prod_with_dayahead.merge(
        df_rmv_prices,
        on=["tech", "year", "month"],
        how="left",
    )

    # Calculate delta (keep power_kwh name for consistency)

    df_prod_with_prices_dedup = df_prod_with_prices.drop_duplicates(
        subset=["malo", "time_berlin", "power_kwh"]
    )

    df_prod_with_prices_dedup["deltaspot_eur"] = (
        df_prod_with_prices_dedup["power_kwh"]
        * df_prod_with_prices_dedup["dayaheadprice"]
        / 1000
    ) - (
        df_prod_with_prices_dedup["power_kwh"]
        * df_prod_with_prices_dedup["monthly_reference_market_price_eur_mwh"]
        / 1000
    )

    # Calculate weighted delta per malo

    df_weighted_delta_by_malo = (
        df_prod_with_prices_dedup.groupby(["malo"])
        .agg(
            total_prod_kwh_malo=("power_kwh", "sum"),
            spot_rmv_eur_malo=("deltaspot_eur", "sum"),
        )
        .reset_index()
    )

    df_weighted_delta_by_malo["weighted_delta_permalo"] = (
        df_weighted_delta_by_malo["spot_rmv_eur_malo"]
        / (df_weighted_delta_by_malo["total_prod_kwh_malo"] / 1000)
    ).round(2)

    # Calculate capacity factors

    total_prod = df_prod_with_prices_dedup.groupby(["malo"])["power_kwh"].sum()

    df_monthly_delta = (
        df_prod_with_prices_dedup.groupby(["year", "month", "malo"])
        .agg(
            deltaspot_eur_monthly=("deltaspot_eur", "sum"),
            available_months=("available_months", "first"),
            available_years=("available_years", "first"),
        )
        .reset_index()
    )

    df_monthly_delta["total_prod_kwh"] = df_monthly_delta["malo"].map(total_prod)

    df_monthly_delta["total_prod_mwh"] = df_monthly_delta["total_prod_kwh"] / 1000

    df_capacity_inputs_by_malo = (
        df_monthly_delta.groupby(["malo"], dropna=False)
        .agg(
            available_months=("available_months", "first"),
            available_years=("available_years", "first"),
            total_prod_mwh=("total_prod_mwh", "first"),
        )
        .reset_index()
    )

    # Merge with main data

    for df in [
        df_assets_enriched,
        df_weighted_delta_by_malo,
        df_capacity_inputs_by_malo,
    ]:

        df["malo"] = df["malo"].astype(str).str.strip()

    df_assets_with_weighted_delta = pd.merge(
        df_assets_enriched,
        df_weighted_delta_by_malo[
            ["malo", "weighted_delta_permalo", "total_prod_kwh_malo"]
        ],
        on="malo",
        how="left",
    )

    df_assets_with_production_metrics = pd.merge(
        df_assets_with_weighted_delta, df_capacity_inputs_by_malo, on="malo", how="left"
    )

    df_assets_with_production_metrics["denominator"] = (
        df_assets_with_production_metrics["Power in MW"]
        * df_assets_with_production_metrics["available_months"]
        * 730
    )

    df_assets_with_production_metrics["denominator"] = (
        df_assets_with_production_metrics["denominator"].replace(0, float("nan"))
    )

    df_assets_with_production_metrics["capacity_factor_percent"] = (
        (df_assets_with_production_metrics["total_prod_mwh"])
        / df_assets_with_production_metrics["denominator"]
        * 100
    ).round(2)

    # =========================================================================

    # STEP 8: ML CURTAILMENT FORECASTING (if data for 2023-2025)

    # =========================================================================

    print_header("STEP 8: CHECKING FOR CURTAILMENT FORECASTING")

    # Check if we have recent data for forecasting

    has_recent_data = (
        df_prod_with_prices["time_berlin"].dt.year.isin([2023, 2024, 2025]).any()
    )

    df_out_process = None

    forecast_curt_weighted_delta_permalo = None

    forecast_curt_year_agg = None

    if has_recent_data:

        print("✅ Recent data (2023-2025) found, proceeding with ML forecasting")

        # Filter to 2023-2025 (keep power_kwh column name)

        df_forecast_input = df_prod_with_prices[
            df_prod_with_prices["time_berlin"].dt.year.isin([2023, 2024, 2025])
        ].copy()

        # Exclude malos with curtailment flag (already have curtailment data)

        if "curtailment" in df_production_filtered.columns:

            curtailed_malos = df_production_filtered[
                df_production_filtered["curtailment"]
            ]["malo"].unique()

            df_forecast_input = df_forecast_input[
                ~df_forecast_input["malo"].isin(curtailed_malos)
            ]

            print(
                f"ℹ️ Excluded {len(curtailed_malos)} malos with existing curtailment data"
            )

        if not df_forecast_input.empty:

            try:

                # Fetch forecast table from BigQuery

                print("Fetching forecast features from BigQuery...")

                forecast_table_query = """

                SELECT *

                FROM `flex-power.sales.price_volume_data_for_curtailment_forecast_table`

                ORDER BY delivery_start_berlin

                """

                df_forecast_features = pandas_gbq.read_gbq(
                    forecast_table_query, project_id=PROJECT_ID
                )

                df_forecast_features["delivery_start_berlin"] = pd.to_datetime(
                    df_forecast_features["delivery_start_berlin"], errors="coerce"
                )

                # Merge forecast features (avoid column name conflicts)

                # Keep only columns from forecast table that don't exist in
                # df_forecast_input

                forecast_feature_cols = [
                    col
                    for col in df_forecast_features.columns
                    if col not in df_forecast_input.columns
                    or col in ["delivery_start_berlin", "tech"]
                ]

                df_forecast_input = df_forecast_input.merge(
                    df_forecast_features[forecast_feature_cols],
                    left_on=["time_berlin", "tech"],
                    right_on=["delivery_start_berlin", "tech"],
                    how="left",
                )

                print(f"✅ Merged forecast features: {len(df_forecast_input)} rows")

                print(
                    f"ℹ️ Forecast input has power_kwh column: {'power_kwh' in df_forecast_input.columns}"
                )

                # Run ML forecasting pipeline

                results = run_curtailment_forecast_multi_category(
                    df_ts=df_forecast_input,
                    plot_class=False,
                    plot_reg=False,
                )

                if results is not None:

                    df_curtailment_forecast_predictions = results["combined"]

                    print(
                        f"✅ Curtailment forecasting complete: {len(df_curtailment_forecast_predictions)} predictions"
                    )

                    # Save forecast results

                    save_multisheet_excel(
                        df_curtailment_forecast_predictions,
                        str(folder / "df_out_forecast_results.xlsx"),
                    )

                    # Process forecast data

                    df_curtailment_forecast_predictions.rename(
                        columns={"dayaheadprice_eur_mwh": "dayaheadprice"}, inplace=True
                    )

                    df_curtailment_forecast_predictions["curtailment_forecast_kwh"] = (
                        df_curtailment_forecast_predictions[
                            "predicted_curtailment_kWh_per_kw"
                        ]
                        * df_curtailment_forecast_predictions["net_power_kw_unit"]
                    )

                    df_curtailment_forecast_predictions["power_kwh"] = (
                        df_curtailment_forecast_predictions["power_kwh"]
                        + df_curtailment_forecast_predictions[
                            "curtailment_forecast_kwh"
                        ]
                    )

                    df_forecast_prod_for_metrics = df_curtailment_forecast_predictions[
                        [
                            "malo",
                            "time_berlin",
                            "power_kwh",
                            "dayaheadprice",
                            "monthly_reference_market_price_eur_mwh",
                            "available_years",
                            "available_months",
                            "year",
                            "month",
                        ]
                    ]

                    # Process forecast production data

                    forecast_curt_delta = process_production_data(
                        df_forecast_prod_for_metrics, folder
                    )

                    df_forecast_weighted_delta_by_malo = forecast_curt_delta[
                        "weighted_delta_permalo"
                    ]

                    df_forecast_capacity_inputs_by_malo = forecast_curt_delta[
                        "year_agg"
                    ]

                    # Rename columns

                    df_forecast_weighted_delta_by_malo.rename(
                        columns={
                            "weighted_delta_permalo": "forecast_weighted_delta_permalo",
                            "total_prod_kwh_malo": "forecast_total_prod_kwh_malo",
                        },
                        inplace=True,
                    )

                    df_forecast_capacity_inputs_by_malo.rename(
                        columns={
                            "available_months": "forecast_available_months",
                            "available_years": "forecast_available_years",
                            "total_prod_mwh": "forecast_total_prod_mwh",
                        },
                        inplace=True,
                    )

                    print("✅ Forecast data processed and ready for merge")

            except Exception as e:

                print(f"⚠️ Curtailment forecasting failed: {str(e)}")

                print("Continuing without forecast data...")

        else:

            print("ℹ️ No data available for forecasting after filtering")

    else:

        print("ℹ️ No recent data (2023-2025) for curtailment forecasting")

    # =========================================================================

    # STEP 9: Merge forecast results with main data

    # =========================================================================

    if (
        "df_forecast_weighted_delta_by_malo" in locals()
        and "df_forecast_capacity_inputs_by_malo" in locals()
        and df_forecast_weighted_delta_by_malo is not None
        and df_forecast_capacity_inputs_by_malo is not None
    ):

        print_header("STEP 9: MERGING FORECAST RESULTS")

        # Ensure malo is string

        for df in [
            df_forecast_weighted_delta_by_malo,
            df_forecast_capacity_inputs_by_malo,
        ]:

            df["malo"] = df["malo"].astype(str).str.strip()

        df_assets_with_forecast_delta = pd.merge(
            df_assets_with_production_metrics,
            df_forecast_weighted_delta_by_malo[
                [
                    "malo",
                    "forecast_weighted_delta_permalo",
                    "forecast_total_prod_kwh_malo",
                ]
            ],
            on="malo",
            how="left",
        )

        df_assets_with_forecast_metrics = pd.merge(
            df_assets_with_forecast_delta,
            df_forecast_capacity_inputs_by_malo,
            on="malo",
            how="left",
        )

        df_assets_with_forecast_metrics["denominator_1"] = (
            df_assets_with_forecast_metrics["Power in MW"]
            * df_assets_with_forecast_metrics["forecast_available_months"]
            * 730
        )

        df_assets_with_forecast_metrics["denominator_1"] = (
            df_assets_with_forecast_metrics["denominator_1"].replace(0, float("nan"))
        )

        df_assets_with_forecast_metrics["forecast_capacity_factor_percent"] = (
            (df_assets_with_forecast_metrics["forecast_total_prod_kwh_malo"])
            / 1000
            / df_assets_with_forecast_metrics["denominator_1"]
            * 100
        ).round(2)

        df_report_input = df_assets_with_forecast_metrics

        has_forecast = True

        print("✅ Forecast results merged")

    else:

        df_report_input = df_assets_with_production_metrics

        has_forecast = False

        print("ℹ️ No forecast results to merge")

    # =========================================================================

    # STEP 10: Generate final reports

    # =========================================================================

    print_header("STEP 10: GENERATING REPORTS")

    sheet1, sheet2, sheet3 = generate_output_sheets(
        df_report_input, has_production=True, has_forecast=has_forecast
    )

    customer_name = os.path.basename(os.path.dirname(out_path))

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        sheet1.to_excel(writer, sheet_name=f"{customer_name}_1", index=False)
        sheet2.to_excel(writer, sheet_name=f"{customer_name}_2", index=False)
        sheet3.to_excel(writer, sheet_name=f"{customer_name}_3", index=False)

    format_excel_output(out_path)

    print_header("PIPELINE COMPLETE")

    print(f"✅ Output saved to {out_path}")
    # print(f"✅ Formatted output saved to {out_path_highlighted}")

    # Print summary of scenarios processed

    print_header("SCENARIOS PROCESSED")

    print(f"✅ SEE data: {'Yes' if df_see_units is not None and not df_see_units.empty else 'No'}")
    print(f"✅ SEE Enervis: {'Yes' if df_assets_see_enriched is not None else 'No'}")
    print(
        f"✅ Non-SEE Enervis: {'Yes' if df_assets_nonsee_enervis_enriched is not None else 'No'}"
    )
    print(f"✅ Production data: {'Yes' if has_production else 'No'}")
    print(
        f"✅ Curtailment data: {'Yes' if 'curtailment' in available_sheets else 'No'}"
    )
    print(f"✅ Redispatch data: {'Yes' if 'redispatch' in available_sheets else 'No'}")
    print(f"✅ ML Forecasting: {'Yes' if has_forecast else 'No'}")


if __name__ == "__main__":
    main()

import pandas as pd
import numpy as np

print("VERSION: full extraction — deposits + lending rates")
print("Starting transformation...")

df = pd.read_csv("data/raw/raw_interest_rates.csv")

# -------------------------
# CONSTANTS
# -------------------------

BANK_NAMES = [
    "ACCESS BANK", "ALPHA MORGAN BANK", "CITI BANK",
    "CORONATION MERCHANT BANK", "ECOBANK", "FBN QUEST MERCHANT BANK",
    "FCMB", "FIDELITY BANK", "FIRST BANK OF NIGERIA",
    "FSDH MERCHANT BANK", "GLOBUS BANK LTD", "GREENWICH MERCHANT BANK",
    "GUARANTY TRUST BANK", "KEYSTONE BANK LTD", "NOVA BANK",
    "OPTIMUS BANK", "PARALLEX BANK", "POLARIS BANK",
    "PREMIUM TRUST BANK", "PROVIDUS BANK", "RAND MERCHANT BANK NIG. LTD",
    "SIGNATURE BANK", "STANBIC IBTC", "STANDARD CHARTERED BANK",
    "STERLING BANK", "SUNTRUST BANK", "TATUM BANK",
    "UNITED BANK FOR AFRICA", "UNION BANK", "UNITY BANK",
    "WEMA BANK", "ZENITH BANK",
]

REPORTING_DATE = "2026-02-06"

# Row index → (sector, rate_type)
# Even rows = PRIME, odd NaN rows = MAX
SECTOR_ROWS = {
    10: "AGRICULTURE, FORESTRY AND FISHING",
    12: "MINING & QUARRYING",
    14: "MANUFACTURING",
    16: "REAL ESTATE ACTIVITIES",
    18: "PUBLIC UTILITIES",
    20: "GENERAL COMMERCE",
    22: "TRANSPORTATION & STORAGE",
    24: "FINANCE & INSURANCE",
    26: "GENERAL",
    28: "GOVERNMENT",
    30: "WATER SUPPLY, SEWAGE, WASTE MANAGEMENT AND REMEDIATION",
    32: "CONSTRUCTION",
    34: "INFORMATION AND COMMUNICATION",
    36: "PROFESSIONAL, SCIENTIFIC AND TECHNICAL ACTIVITIES",
    38: "ADMINISTRATIVE AND SUPPORT SERVICE ACTIVITIES",
    40: "EDUCATION",
    42: "HUMAN HEALTH AND SOCIAL WORK ACTIVITIES",
    44: "ARTS, ENTERTAINMENT AND RECREATION",
    46: "ACTIVITIES OF EXTRATERRITORIAL ORGANIZATIONS AND BODIES",
    48: "POWER AND ENERGY",
    50: "CAPITAL MARKET",
    52: "OIL & GAS",
}

data_cols = list(df.columns[1:-1])  # skip col 0 and source_page

# -------------------------
# HELPER: clean a rate value
# -------------------------
def clean_rate(value):
    if pd.isna(value):
        return None
    s = str(value).replace(" ", "").strip()
    if s == "-" or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None

# -------------------------
# STEP 1: Deposit rates
# -------------------------
deposit_rows = {
    "demand_deposit_rate": 5,
    "savings_deposit_rate": 6,
    "time_deposit_rate": 7,
}

deposit_records = []
for i, bank in enumerate(BANK_NAMES):
    if i >= len(data_cols):
        break
    col = data_cols[i]
    record = {
        "bank_name": bank,
        "reporting_date": REPORTING_DATE,
    }
    for rate_name, row_idx in deposit_rows.items():
        record[rate_name] = clean_rate(df.iloc[row_idx][col])
    deposit_records.append(record)

deposit_df = pd.DataFrame(deposit_records)
print(f"Deposit rows: {len(deposit_df)}")

# -------------------------
# STEP 2: Lending rates
# -------------------------
lending_records = []
for prime_row, sector in SECTOR_ROWS.items():
    max_row = prime_row + 1  # NaN row always holds MAX values
    for i, bank in enumerate(BANK_NAMES):
        if i >= len(data_cols):
            break
        col = data_cols[i]
        lending_records.append({
            "bank_name": bank,
            "sector": sector,
            "prime_rate": clean_rate(df.iloc[prime_row][col]),
            "max_rate": clean_rate(df.iloc[max_row][col]),
            "reporting_date": REPORTING_DATE,
        })

lending_df = pd.DataFrame(lending_records)
print(f"Lending rows: {len(lending_df)}")

# -------------------------
# STEP 3: Save
# -------------------------
deposit_df.to_csv("data/processed/clean_deposit_rates.csv", index=False)
lending_df.to_csv("data/processed/clean_lending_rates.csv", index=False)

print("Transformation complete.")
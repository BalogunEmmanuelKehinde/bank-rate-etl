import pandas as pd
from sqlalchemy import create_engine, text

print("VERSION: two table load — deposits + lending rates")

DB_USER = "postgres"
DB_PASSWORD = "elric"  # replace this
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "cbn_rates"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -------------------------
# STEP 1: Create tables
# -------------------------
ddl = """
CREATE TABLE IF NOT EXISTS deposit_rates (
    id                   SERIAL PRIMARY KEY,
    bank_name            VARCHAR(100) NOT NULL,
    demand_deposit_rate  NUMERIC(6,2),
    savings_deposit_rate NUMERIC(6,2),
    time_deposit_rate    NUMERIC(6,2),
    reporting_date       DATE NOT NULL,
    created_at           TIMESTAMP DEFAULT NOW(),
    UNIQUE (bank_name, reporting_date)
);

CREATE TABLE IF NOT EXISTS lending_rates (
    id             SERIAL PRIMARY KEY,
    bank_name      VARCHAR(100) NOT NULL,
    sector         VARCHAR(100) NOT NULL,
    prime_rate     NUMERIC(6,2),
    max_rate       NUMERIC(6,2),
    reporting_date DATE NOT NULL,
    created_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (bank_name, sector, reporting_date)
);
"""

with engine.connect() as conn:
    conn.execute(text(ddl))
    conn.commit()
print("Tables ready.")

# -------------------------
# STEP 2: Load deposits
# -------------------------
deposit_df = pd.read_csv("data/processed/clean_deposit_rates.csv")
deposit_df["reporting_date"] = pd.to_datetime(deposit_df["reporting_date"])

deposit_sql = """
INSERT INTO deposit_rates (bank_name, demand_deposit_rate, savings_deposit_rate, time_deposit_rate, reporting_date)
VALUES (:bank_name, :demand_deposit_rate, :savings_deposit_rate, :time_deposit_rate, :reporting_date)
ON CONFLICT (bank_name, reporting_date) DO NOTHING;
"""

with engine.connect() as conn:
    for _, row in deposit_df.iterrows():
        conn.execute(text(deposit_sql), {
            "bank_name": row["bank_name"],
            "demand_deposit_rate": None if pd.isna(row["demand_deposit_rate"]) else row["demand_deposit_rate"],
            "savings_deposit_rate": None if pd.isna(row["savings_deposit_rate"]) else row["savings_deposit_rate"],
            "time_deposit_rate": None if pd.isna(row["time_deposit_rate"]) else row["time_deposit_rate"],
            "reporting_date": row["reporting_date"],
        })
    conn.commit()
print(f"Loaded {len(deposit_df)} deposit rows.")

# -------------------------
# STEP 3: Load lending rates
# -------------------------
lending_df = pd.read_csv("data/processed/clean_lending_rates.csv")
lending_df["reporting_date"] = pd.to_datetime(lending_df["reporting_date"])

lending_sql = """
INSERT INTO lending_rates (bank_name, sector, prime_rate, max_rate, reporting_date)
VALUES (:bank_name, :sector, :prime_rate, :max_rate, :reporting_date)
ON CONFLICT (bank_name, sector, reporting_date) DO NOTHING;
"""

with engine.connect() as conn:
    for _, row in lending_df.iterrows():
        conn.execute(text(lending_sql), {
            "bank_name": row["bank_name"],
            "sector": row["sector"],
            "prime_rate": None if pd.isna(row["prime_rate"]) else row["prime_rate"],
            "max_rate": None if pd.isna(row["max_rate"]) else row["max_rate"],
            "reporting_date": row["reporting_date"],
        })
    conn.commit()
print(f"Loaded {len(lending_df)} lending rows.")

# -------------------------
# STEP 4: Verify
# -------------------------
with engine.connect() as conn:
    d = pd.read_sql("SELECT * FROM deposit_rates LIMIT 3", conn)
    l = pd.read_sql("SELECT * FROM lending_rates LIMIT 3", conn)
    print("\nDeposit preview:")
    print(d.to_string())
    print("\nLending preview:")
    print(l.to_string())

print("Load complete.")
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np

app = FastAPI(title="CBN Bank Rates API")

DB_USER = "postgres"
DB_PASSWORD = "elric"  # replace this
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "cbn_rates"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

import json

def clean_records(df):
    """Replace NaN with None so JSON serialization works."""
    return json.loads(df.to_json(orient="records"))

# -------------------------
# DEPOSIT ENDPOINTS
# -------------------------

@app.get("/rates/deposits")
def get_all_deposits():
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM deposit_rates ORDER BY bank_name", conn)
    return clean_records(df)

@app.get("/rates/deposits/{bank_name}")
def get_bank_deposits(bank_name: str):
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT * FROM deposit_rates WHERE LOWER(bank_name) = LOWER(:bank_name)"),
            conn,
            params={"bank_name": bank_name}
        )
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Bank '{bank_name}' not found")
    return clean_records(df)

# -------------------------
# LENDING ENDPOINTS
# -------------------------

@app.get("/rates/lending")
def get_all_lending():
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM lending_rates ORDER BY bank_name, sector", conn)
    return clean_records(df)

@app.get("/rates/lending/{bank_name}")
def get_bank_lending(bank_name: str):
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT * FROM lending_rates WHERE LOWER(bank_name) = LOWER(:bank_name) ORDER BY sector"),
            conn,
            params={"bank_name": bank_name}
        )
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Bank '{bank_name}' not found")
    return clean_records(df)
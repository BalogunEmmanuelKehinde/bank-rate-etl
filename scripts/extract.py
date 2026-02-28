print("Script started...")

import pdfplumber
import pandas as pd
import os

pdf_path = os.path.join("data", "raw", "weekly_interest_rates_2026_02_06.pdf")

print("Looking for file:", pdf_path)
print("File exists:", os.path.exists(pdf_path))

all_tables = []

with pdfplumber.open(pdf_path) as pdf:
    print("PDF opened successfully.")
    for page_number, page in enumerate(pdf.pages):
        print(f"Processing page {page_number + 1}")
        tables = page.extract_tables()
        print(f"Tables found on page {page_number + 1}: {len(tables)}")
        for table in tables:
            df = pd.DataFrame(table)
            df["source_page"] = page_number + 1
            all_tables.append(df)

if all_tables:
    raw_df = pd.concat(all_tables, ignore_index=True)
    raw_df.to_csv("data/raw/raw_interest_rates.csv", index=False)
    print("Extraction complete.")
else:
    print("No tables found.")
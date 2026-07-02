import csv
import os
import re
from pathlib import Path

import psycopg2


CSV_PATH = Path("rafah/modelling/market_flow_outputs/market_flow_joined_dataset.csv")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_DB = os.getenv("PG_DB", "kurs_eur_db")
PG_USER = os.getenv("PG_USER", "kursadmin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "kursadmin")


def normalize_col(name):
    name = name.strip()
    name = re.sub(r"[^a-zA-Z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.lower().strip("_")


def q(name):
    return '"' + name.replace('"', '""') + '"'


if not CSV_PATH.exists():
    raise FileNotFoundError(f"Missing CSV: {CSV_PATH}")

with CSV_PATH.open("r", encoding="utf-8") as f:
    reader = csv.reader(f)
    original_columns = next(reader)

columns = [normalize_col(c) for c in original_columns]

print("[INFO] Column mapping:")
for old, new in zip(original_columns, columns):
    print(f"  {old} -> {new}")

column_defs = []
for col in columns:
    if col == "date":
        column_defs.append(f"{q(col)} date")
    else:
        column_defs.append(f"{q(col)} double precision")

conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD,
)

with conn:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS market_flow_joined;")
        cur.execute(f"""
            CREATE TABLE market_flow_joined (
                {", ".join(column_defs)}
            );
        """)

        copy_sql = f"""
            COPY market_flow_joined ({", ".join(q(c) for c in columns)})
            FROM STDIN
            WITH CSV HEADER
        """

        with CSV_PATH.open("r", encoding="utf-8") as f:
            cur.copy_expert(copy_sql, f)

        cur.execute("SELECT COUNT(*) FROM market_flow_joined;")
        count = cur.fetchone()[0]

print(f"[OK] Loaded {count} rows into table market_flow_joined")

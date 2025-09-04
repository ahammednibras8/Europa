import glob
import os
from app.database import con

from app.schema import create_table_with_schema, detect_schema

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

def load_csv_files():
    csv_file = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    for file in csv_file:
        table_name = os.path.splitext(os.path.basename(file))[0]
        schema = detect_schema(file, "csv")
        create_table_with_schema(table_name, file, schema, "csv")

def load_parquet_files():
    parquet_files = glob.glob(os.path.join(DATA_DIR, "*.parquet"))
    for file in parquet_files:
        table_name = os.path.splitext(os.path.basename(file))[0]
        schema = detect_schema(file, "parquet")
        create_table_with_schema(table_name, file, schema, "parquet")

def load_json_files():
    json_files = glob.glob(os.path.join(DATA_DIR, "*.json"))
    for file in json_files:
        table_name = os.path.splitext(os.path.basename(file))[0]
        schema = detect_schema(file, "json")
        create_table_with_schema(table_name, file, schema, "json")

def preview_table(table_name: str, limit: int = 5):
    try:
        result = con.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchdf()
        return result.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

def load_all_data():
    load_csv_files()
    load_parquet_files()
    load_json_files()
    print("All files loaded into DuckDB successfully.")
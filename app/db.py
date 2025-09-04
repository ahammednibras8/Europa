import glob
import os
import duckdb

con = duckdb.connect(database='europa.db')

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
print("DATA_DIR:", DATA_DIR)

def load_csv_files():
    csv_file = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    for file in csv_file:
        table_name = os.path.splitext(os.path.basename(file))[0]
        print(f"Loading CSV: {file} -> Table: {table_name}")
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file}')")

def load_parquet_files():
    parquet_files = glob.glob(os.path.join(DATA_DIR, "*.parquet"))
    for file in parquet_files:
        table_name = os.path.splitext(os.path.basename(file))[0]
        print(f"Loading Parquet: {file} → Table: {table_name}")
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{file}')")

def load_json_files():
    json_files = glob.glob(os.path.join(DATA_DIR, "*.json"))
    for file in json_files:
        table_name = os.path.splitext(os.path.basename(file))[0]
        print(f"Loading JSON: {file} -> Table: {table_name}")
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto('{file}')")

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
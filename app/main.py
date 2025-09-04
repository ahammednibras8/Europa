from fastapi import FastAPI
from app.db import load_all_data, con, preview_table

app = FastAPI(title="Europa - Create Your own insights")

@app.on_event("startup")
def startup_event():
    load_all_data()

@app.get("/table")
def list_tables():
    tables = con.execute("SHOW TABLES").fetchall()
    return {"tables": [t[0] for t in tables]}

@app.get("/preview/{table_name}")
def preview(table_name: str, limit: int = 5):
    return preview_table(table_name, limit)

@app.get("/schema/{table_name}")
def get_schema(table_name: str):
    try:
        result = con.execute(f"DESCRIBE {table_name}").fetchall()
        return [{"name": col[0], "type": col[1]} for col in result]
    except Exception as e:
        return {"error": str(e)}
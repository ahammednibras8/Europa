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
    return {"table": table_name, "rows": preview_table(table_name, limit)}
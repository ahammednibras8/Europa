from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.db import load_all_data, preview_table
from app.database import con

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

class SQLQuery(BaseModel):
    query: str

@app.post("/query")
def run_query(payload: SQLQuery):
    sql = payload.query.strip()
    if not sql:
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    try:
        result = con.execute(sql).fetchdf()
        return {
            "query": sql,
            "row_count": len(result),
            "rows": result.to_dict(orient="records"),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.db import load_all_data, preview_table
from app.database import con
from app.logger import logger

app = FastAPI(title="Europa - Create Your own insights")

@app.on_event("startup")
def startup_event():
    load_all_data()

@app.get("/table")
def list_tables():
    logger.info("[ROUTE] /table called")
    try:
        tables = con.execute("SHOW TABLES").fetchall()
    except Exception as e:
        logger.error(f"[ROUTE FAIL] /table failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to list tables")
    
    logger.info(f"[ROUTE SUCCESS] /table returned {len(tables)} tables")
    return {"tables": [t[0] for t in tables]}

@app.get("/preview/{table_name}")
def preview(table_name: str, limit: int = 5):
    return preview_table(table_name, limit)

@app.get("/schema/{table_name}")
def get_schema(table_name: str):
    try:
        result = con.execute(f"DESCRIBE {table_name}").fetchall()
        logger.info(f"[SCHEMA API SUCCESS] Table '{table_name}' has {len(result)} columns")
        return [{"name": col[0], "type": col[1]} for col in result]
    except Exception as e:
        logger.error(f"[SCHEMA API FAIL] Failed to fetch schema for table '{table_name}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch schema for table '{table_name}'")

class SQLQuery(BaseModel):
    query: str

@app.post("/query")
def run_query(payload: SQLQuery):
    sql = payload.query.strip()
    
    if not sql:
        logger.warning("[QUERY WARNING] Received empty SQL query")
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    try:
        result = con.execute(sql).fetchdf()
        row_count = len(result)
        logger.info(f"[QUERY SUCCESS] SQL executed, rows returned: {row_count}")
        return {
            "query": sql,
            "row_count": len(result),
            "rows": result.to_dict(orient="records"),
        }
    except Exception as e:
        logger.error(f"[QUERY FAIL] SQL execution failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))
from fastapi import APIRouter, HTTPException
from app.core.logger import logger
from app.db.database import con
from app.db.db import preview_table

router = APIRouter()

@router.get("/table")
def list_tables():
    logger.info("[ROUTE] /table called")
    try:
        tables = con.execute("SHOW TABLES").fetchall()
    except Exception as e:
        logger.error(f"[ROUTE FAIL] /table failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to list tables")
    
    logger.info(f"[ROUTE SUCCESS] /table returned {len(tables)} tables")
    return {"tables": [t[0] for t in tables]}

@router.get("/preview/{table_name}")
def preview(table_name: str, limit: int = 5):
    return preview_table(table_name, limit)
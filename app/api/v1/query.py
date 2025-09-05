from fastapi import APIRouter, HTTPException

from app.models.query_models import SQLQuery
from app.core.logger import logger
from app.db.database import con

router = APIRouter()

@router.post("/query")
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
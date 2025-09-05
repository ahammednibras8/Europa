from fastapi import APIRouter, HTTPException

from app.models.query_models import NLQuery
from app.core.logger import logger

router = APIRouter()

@router.post("/nlquery")
def receive_nl_query(payload: NLQuery):
    nl_query = payload.query.strip()
    
    if not nl_query:
        logger.warning("[NLQUERY WARNING] Received empty natural language query")
        raise HTTPException(status_code=400, detail="Natural language query cannot be empty")

    logger.info(f"[NLQUERY RECEIVED] NL query received: {nl_query[:100]}...")
    return {"nl_query": nl_query, "status": "received"}
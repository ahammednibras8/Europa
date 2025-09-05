from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.db.db import load_all_data
from app.core.logger import logger
from app.api.v1 import tables, query, nlquery

app = FastAPI(title="Europa - Create Your Own Insights")

@app.on_event("startup")
def startup_event():
    logger.info("[STARTUP] Loading all data")
    load_all_data()

app.include_router(tables.router, prefix="/v1")
app.include_router(query.router, prefix="/v1")
app.include_router(nlquery.router, prefix="/v1")
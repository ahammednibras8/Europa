import glob
import os
from app.database import con
from app.logger import logger

from app.schema import create_table_with_schema, detect_schema

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

def preview_table(table_name: str, limit: int = 5):
    try:
        result = con.execute(f"SELECT * FROM {table_name} LIMIT {limit}").fetchdf()
        rows = result.to_dict(orient="records")
        logger.info(f"[PREVIEW SUCCESS] Table '{table_name}' preview returned {len(rows)} rows")
        return rows
    except Exception as e:
        logger.error(f"[PREVIEW FAIL] Table '{table_name}' preview failed: {e}")
        return {"error": str(e)}

def load_all_data():
    logger.info("[LOAD ALL DATA START] Loading CSV, Parquet, JSON files")
    
    file_types = {
        "csv": "*.csv",
        "parquet": "*.parquet",
        "json": "*.json"
    }
    
    for ftype, pattern in file_types.items():
        files = glob.glob(os.path.join(DATA_DIR, pattern))
        success, failed = [], []
        
        for file in files:
            table_name = os.path.splitext(os.path.basename(file))[0]
            try:
                schema = detect_schema(file, ftype)
                create_table_with_schema(table_name, file, schema, ftype)
                success.append(table_name)
            except Exception as e:
                failed.append((table_name, str(e)))
                
        if success:
            logger.info(f"[{ftype.upper()} SUCCESS] Loaded tables: {', '.join(success)}")
        if failed:
            for t, err in failed:
                logger.error(f"[{ftype.upper()} FAIL] Table '{t}' failed to load: {err}")
    
    logger.info("[LOAD ALL DATA COMPLETE] Data loading finished")
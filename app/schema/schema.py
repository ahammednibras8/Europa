from app.db.database import con
from app.core.logger import logger

def detect_schema(file: str, file_type: str):
    try:
        if file_type == "csv":
            query = f"DESCRIBE SELECT * FROM read_csv_auto('{file}', SAMPLE_SIZE=1000)"
        elif file_type == "parquet":
            query = f"DESCRIBE SELECT * FROM read_parquet('{file}')"
        elif file_type == "json":
            query = f"DESCRIBE SELECT * FROM read_json_auto('{file}')"
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
    
        result = con.execute(query).fetchall()
        logger.info(f"[SCHEMA DETECT SUCCESS] File '{file}' detected {len(result)} columns")
        return [{"name": col[0], "type": col[1]} for col in result]
    
    except Exception as e:
        logger.error(f"[SCHEMA DETECT FAIL] File '{file}' schema detection failed: {e}")
        raise

def create_table_with_schema(table_name: str, file: str, schema: list, file_type: str):
    try:
        cols = ", ".join([f'"{col["name"]}" {col["type"]}' for col in schema])
        con.execute(f"CREATE OR REPLACE TABLE {table_name} ({cols})")
    
        if file_type == "csv":
            con.execute(f"INSERT INTO {table_name} SELECT * FROM read_csv_auto('{file}')")
        elif file_type == "parquet":
            con.execute(f"INSERT INTO {table_name} SELECT * FROM read_parquet('{file}')")
        elif file_type == "json":
            con.execute(f"INSERT INTO {table_name} SELECT * FROM read_json_auto('{file}')")
        
        logger.info(f"[TABLE CREATE SUCCESS] Table '{table_name}' created from file '{file}' with {len(schema)} columns")
        return {"table": table_name, "schema": schema}
    
    except Exception as e:
        logger.error(f"[TABLE CREATE FAIL] Table '{table_name}' from file '{file}' failed: {e}")
        raise
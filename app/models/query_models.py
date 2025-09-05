from pydantic import BaseModel

class SQLQuery(BaseModel):
    query: str

class NLQuery(BaseModel):
    query: str
from fastapi import APIRouter
from pydantic import BaseModel, field_validator
import sqlglot
from config import SUPPORTED_DIALECTS 

router = APIRouter()



class TranspileRequest(BaseModel):
    query: str
    source_dialect: str
    target_dialect: str
    pretty: bool = True 

    @field_validator("source_dialect", "target_dialect")
    def validate_dialect(cls, v):
        if v.lower() not in SUPPORTED_DIALECTS:
            raise ValueError(f"Unsupported dialect '{v}'. Supported dialects are: {', '.join(sorted(SUPPORTED_DIALECTS))}.")
        return v.lower()

@router.post("/transpile")
def transpile_sql(request: TranspileRequest):
    """
    Transpile the given SQL from the source_dialect to the target_dialect
    """
    parsed = sqlglot.parse_one(request.query, read=request.source_dialect)

    transpiled_sql = parsed.sql(
        dialect=request.target_dialect,
        pretty=request.pretty
    )

    return {
        "original_query": request.query,
        "source_dialect": request.source_dialect,
        "target_dialect": request.target_dialect,
        "transpiled_query": transpiled_sql
    }


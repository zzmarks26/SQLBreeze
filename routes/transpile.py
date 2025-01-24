# sqlbreeze/app/routers/transpile.py

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
import sqlglot
from sqlglot.errors import ParseError

router = APIRouter()

class TranspileRequest(BaseModel):
    query: str
    source_dialect: str
    target_dialect: str

@router.post("/transpile")
def transpile_sql(request: TranspileRequest):
    """
    Transpile the given SQL from the source_dialect to the target_dialect
    using SQLGlot. All fields are required (no defaults).
    If parsing fails, return a 400 Bad Request with the error message.
    """
    try:
        parsed = sqlglot.parse_one(request.query, read=request.source_dialect)
        transpiled_sql = parsed.sql(dialect=request.target_dialect)
        return {
            "original_query": request.query,
            "source_dialect": request.source_dialect,
            "target_dialect": request.target_dialect,
            "transpiled_query": transpiled_sql
        }
    except ParseError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Failed to parse SQL: {e}"
        )

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, field_validator, validator
import sqlglot
from sqlglot.errors import ParseError
from config import SUPPORTED_DIALECTS 

router = APIRouter()



class TranspileRequest(BaseModel):
    query: str
    source_dialect: str
    target_dialect: str
    pretty: bool = False 

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
    try:
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

    except ParseError as e:
        # Handle SQL parsing errors specifically
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Failed to parse SQL: {e}"
        )
    except ValueError as ve:
        # Handle validation errors (e.g., unsupported dialects)
        raise HTTPException(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            detail=str(ve)
        )
    except Exception as ex:
        # Catch-all for any other unexpected errors
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"An unexpected error occurred: {ex}"
        )

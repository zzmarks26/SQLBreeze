from fastapi import APIRouter, FastAPI, HTTPException, status
from pydantic import BaseModel
import sqlglot
from sqlglot.errors import ParseError

router = APIRouter()

class FormatRequest(BaseModel):
    query: str
    target: str  # Target dialect for formatting (e.g., "postgres", "mysql")
    pretty: bool = True      # Whether to format the SQL output

class FormatResponse(BaseModel):
    original_query: str
    formatted_query: str

@router.post("/format", response_model=FormatResponse)
def format_sql(request: FormatRequest):
    try:
        # Parse the SQL query
        parsed = sqlglot.parse_one(request.query)
        
        # Format the SQL query
        formatted_query = parsed.sql(dialect=request.target, pretty=request.pretty)
        
        return FormatResponse(
            original_query=request.query,
            formatted_query=formatted_query
        )
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

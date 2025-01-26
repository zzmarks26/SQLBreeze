from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
import sqlglot
from sqlglot.optimizer import optimize
from sqlglot.errors import ParseError

router = APIRouter()

class OptimizationRequest(BaseModel):
    query: str
    target: str = "generic"  # Target dialect for optimization (e.g., "postgres", "mysql")
    data_schema: dict = None      # Optional schema for optimization
    pretty: bool = True      # Format SQL output

class OptimizationResponse(BaseModel):
    original_query: str
    optimized_query: str

@router.post("/optimize", response_model=OptimizationResponse)
def optimize_sql(request: OptimizationRequest):
    try:
        # Parse the SQL query
        parsed = sqlglot.parse_one(request.query)
        
        # Optimize the query
        optimized = optimize(parsed, schema=request.data_schema or {})
        

        optimized_query = optimized.sql(
            dialect=request.target,
            pretty=request.pretty
        )
        
        return OptimizationResponse(
            original_query=request.query,
            optimized_query=optimized_query
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

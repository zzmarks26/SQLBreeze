from fastapi import APIRouter
from pydantic import BaseModel
import sqlglot
from sqlglot.optimizer import optimize
from error import error_handler

router = APIRouter()

class OptimizationRequest(BaseModel):
    query: str
    target: str   
    data_schema: dict = None
    pretty: bool = True

class OptimizationResponse(BaseModel):
    original_query: str
    optimized_query: str

@router.post("/optimize", response_model=OptimizationResponse)
@error_handler
def optimize_sql(request: OptimizationRequest):
    parsed = sqlglot.parse_one(request.query)

    optimized = optimize(parsed, schema=request.data_schema or {})
    
    optimized_query = optimized.sql(
        dialect=request.target,
        pretty=request.pretty
    )
    
    return OptimizationResponse(
        original_query=request.query,
        optimized_query=optimized_query
    )
    

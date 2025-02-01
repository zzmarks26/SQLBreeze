from fastapi import APIRouter
from pydantic import BaseModel
import sqlglot
from error import error_handler

router = APIRouter()

class FormatRequest(BaseModel):
    query: str
    target: str 
    pretty: bool = True 

class FormatResponse(BaseModel):
    original_query: str
    formatted_query: str

@router.post("/format", response_model=FormatResponse)
@error_handler
def format_sql(request: FormatRequest):
    parsed = sqlglot.parse_one(request.query)
    
    formatted_query = parsed.sql(dialect=request.target, pretty=request.pretty)
    
    return FormatResponse(
        original_query=request.query,
        formatted_query=formatted_query
    )
    

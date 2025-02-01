from fastapi import APIRouter
from pydantic import BaseModel
import sqlglot
from error import error_handler

router = APIRouter()

class QueryInput(BaseModel):
    query: str

@router.post("/metadata")
@error_handler
async def extract_metadata(query_input: QueryInput):
    expression = sqlglot.parse_one(query_input.query)

    # Extract metadata from the AST
    metadata = {
            "table_expressions": expression.find_all(sqlglot.expressions.Table),
            "column_expressions": expression.find_all(sqlglot.expressions.Column),
            "function_use": expression.find_all(sqlglot.expressions.Func),
            "aliases": expression.find_all(sqlglot.expressions.Alias),
            "where_statements": expression.find_all(sqlglot.expressions.Where),
            "joins": expression.find_all(sqlglot.expressions.Join),
            "groupbys": expression.find_all(sqlglot.expressions.Group),
            "orderbys": expression.find_all(sqlglot.expressions.Order),
            "limits": expression.find_all(sqlglot.expressions.Limit),
            "subqueries": expression.find_all(sqlglot.expressions.Subquery),
            "unions": expression.find_all(sqlglot.expressions.Union),
            "cte": expression.find_all(sqlglot.expressions.CTE),
            "having": expression.find_all(sqlglot.expressions.Having),
            "distinct": expression.find_all(sqlglot.expressions.Distinct),
            "case": expression.find_all(sqlglot.expressions.Case),
            "literals": expression.find_all(sqlglot.expressions.Literal),
        }

    # Format metadata as strings
    formatted_metadata = {key: [str(item) for item in value] for key, value in metadata.items()}

    return {"metadata": formatted_metadata}
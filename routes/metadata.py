from fastapi import APIRouter, FastAPI, HTTPException
from pydantic import BaseModel
import sqlglot

router = APIRouter()

class QueryInput(BaseModel):
    query: str

@router.post("/metadata")
async def extract_metadata(query_input: QueryInput):
    try:
        # Parse the SQL query
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

        # Format metadata as strings for response
        formatted_metadata = {key: [str(item) for item in value] for key, value in metadata.items()}

        return {"metadata": formatted_metadata}

    except sqlglot.errors.ParseError as e:
        raise HTTPException(status_code=400, detail=f"SQL parsing error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

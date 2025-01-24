from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field, field_validator
import sqlglot
from sqlglot.errors import ParseError
from sqlglot.expressions import (
    Table,
    Column,
    Select,
    Join,
    Func,
    Subquery,
    Alias,
    Expression,
)
from typing import Any, Dict, List, Optional
from config import SUPPORTED_DIALECTS  # Ensure this is defined in your config

router = APIRouter()

# ----------------------------
# Pydantic Models
# ----------------------------

class MetadataRequest(BaseModel):
    query: str = Field(..., example="SELECT id, name FROM users WHERE active = 1")
    dialect: Optional[str] = Field(None, example="postgres")  # e.g., "postgres", "mysql"

    @field_validator("dialect", mode="before")
    def set_default_dialect(cls, v):
        return v.lower() if v else "postgres"  # Set to your preferred default dialect

    @field_validator("dialect", mode="after")
    def validate_dialect(cls, v):
        if v.lower() not in SUPPORTED_DIALECTS:
            raise ValueError(
                f"Unsupported dialect '{v}'. Supported dialects are: {', '.join(sorted(SUPPORTED_DIALECTS))}."
            )
        return v.lower()



class ColumnMetadata(BaseModel):
    name: str
    table: Optional[str] = None
    alias: Optional[str] = None
    expression: Optional[str] = None  # The expression defining the column, e.g., "COUNT(*)"


class TableMetadata(BaseModel):
    name: str
    alias: Optional[str] = None
    schema_name: Optional[str] = None  # If schema information is available


class FunctionMetadata(BaseModel):
    name: str
    arguments: List[str]
    alias: Optional[str] = None


class JoinMetadata(BaseModel):
    type: str  # e.g., INNER, LEFT, RIGHT, FULL
    table: str
    alias: Optional[str] = None
    condition: Optional[str] = None


class MetadataResponse(BaseModel):
    tables: List[TableMetadata] = []
    columns: List[ColumnMetadata] = []
    functions: List[FunctionMetadata] = []
    joins: List[JoinMetadata] = []
    subqueries: List[Dict[str, Any]] = []  



def extract_metadata(query: str, dialect: Optional[str] = None) -> MetadataResponse:
    metadata = MetadataResponse()

    try:
        parsed = sqlglot.parse_one(query, read=dialect)
    except ParseError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Failed to parse SQL: {e}"
        )

    # Extract tables
    tables = parsed.find_all(Table)
    for table in tables:
        table_name = table.name
        table_alias = table.alias
        table_schema = table.args.get("db")  # Database/schema if available
        metadata.tables.append(
            TableMetadata(
                name=table_name,
                alias=table_alias,
                schema=table_schema,
            )
        )

    # Extract columns and functions
    select_expression = parsed.find(Select)
    if select_expression:
        for projection in select_expression.expressions:
            # Handle Aliases
            if isinstance(projection, Alias):
                alias_name = projection.alias
                expression = projection.this  # The underlying expression

                if isinstance(expression, Func):
                    # It's a function with an alias
                    func_name = expression.name
                    func_args = [arg.sql() for arg in expression.args.get("expressions", [])]
                    metadata.functions.append(
                        FunctionMetadata(
                            name=func_name,
                            arguments=func_args,
                            alias=alias_name,
                        )
                    )
                    # Also add to columns with expression
                    metadata.columns.append(
                        ColumnMetadata(
                            name=alias_name,
                            table=None,
                            alias=alias_name,
                            expression=expression.sql(),
                        )
                    )
                else:
                    # It's an expression with an alias, but not a function
                    metadata.columns.append(
                        ColumnMetadata(
                            name=alias_name,
                            table=None,
                            alias=alias_name,
                            expression=expression.sql(),
                        )
                    )
            elif isinstance(projection, Func):
                # It's a function without an alias
                func_name = projection.name
                func_args = [arg.sql() for arg in projection.args.get("expressions", [])]
                metadata.functions.append(
                    FunctionMetadata(
                        name=func_name,
                        arguments=func_args,
                        alias=None,
                    )
                )
                # Also add to columns without alias
                metadata.columns.append(
                    ColumnMetadata(
                        name=func_name,
                        table=None,
                        alias=None,
                        expression=projection.sql(),
                    )
                )
            elif isinstance(projection, Column):
                # It's a simple column
                column_name = projection.name
                column_table = projection.table
                column_alias = projection.alias
                metadata.columns.append(
                    ColumnMetadata(
                        name=column_name,
                        table=column_table,
                        alias=column_alias,
                        expression=None,
                    )
                )
            else:
                # Handle other types of expressions
                metadata.columns.append(
                    ColumnMetadata(
                        name=projection.alias_or_name,
                        table=None,
                        alias=projection.alias,
                        expression=projection.sql(),
                    )
                )

    # Extract joins
    joins = parsed.find_all(Join)
    for join in joins:
        join_type = join.kind.upper() if join.kind else "INNER"
        join_table_expr = join.args.get("this")
        if isinstance(join_table_expr, Table):
            join_table = join_table_expr.name
            join_alias = join_table_expr.alias
        else:
            join_table = join_table_expr.sql()
            join_alias = None

        join_condition_expr = join.args.get("on")
        join_condition = join_condition_expr.sql() if join_condition_expr else None

        metadata.joins.append(
            JoinMetadata(
                type=join_type,
                table=join_table,
                alias=join_alias,
                condition=join_condition,
            )
        )

    # Extract subqueries
    subqueries = parsed.find_all(Subquery)
    for subquery in subqueries:
        subquery_sql = subquery.sql()
        metadata.subqueries.append({"subquery": subquery_sql})

    return metadata



@router.post("/metadata", response_model=MetadataResponse)
def get_metadata(request: MetadataRequest):
    """
    Extract and return metadata from the provided SQL query.
    """
    metadata = extract_metadata(request.query, request.dialect)
    return metadata

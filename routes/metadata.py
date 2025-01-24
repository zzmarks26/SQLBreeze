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
    Identifier,
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
    catalog_name: Optional[str] = Field(None, alias="catalog")
    schema_name: Optional[str] = Field(None, alias="schema")


class FunctionMetadata(BaseModel):
    name: str
    arguments: List[str]
    alias: Optional[str] = None


class JoinMetadata(BaseModel):
    type: str  # e.g., INNER, LEFT, RIGHT, FULL
    left_table: str
    right_table: str
    left_alias: Optional[str] = None
    right_alias: Optional[str] = None
    condition: Optional[str] = None



class MetadataResponse(BaseModel):
    tables: List[TableMetadata] = []
    columns: List[ColumnMetadata] = []
    functions: List[FunctionMetadata] = []
    joins: List[JoinMetadata] = []
    subqueries: List[Dict[str, Any]] = []  



from fastapi import HTTPException, status
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
    catalog_name: Optional[str] = Field(None, alias="catalog")
    schema_name: Optional[str] = Field(None, alias="schema")

    class Config:
        populate_by_name = True  # Allows using 'schema_name' internally



class FunctionMetadata(BaseModel):
    name: str
    arguments: List[str]
    alias: Optional[str] = None


class JoinMetadata(BaseModel):
    type: str  # e.g., INNER, LEFT, RIGHT, FULL
    left_table: str
    right_table: str
    left_alias: Optional[str] = None
    right_alias: Optional[str] = None
    condition: Optional[str] = None


class MetadataResponse(BaseModel):
    tables: List[TableMetadata] = []
    columns: List[ColumnMetadata] = []
    functions: List[FunctionMetadata] = []
    joins: List[JoinMetadata] = []
    subqueries: List[Dict[str, Any]] = []  # You can define a more detailed model if needed


# ----------------------------
# Metadata Extraction Logic
# ----------------------------


def extract_metadata(query: str, dialect: Optional[str] = None) -> MetadataResponse:
    metadata = MetadataResponse()

    try:
        parsed = sqlglot.parse_one(query, read=dialect)
    except ParseError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Failed to parse SQL: {e}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}"
        )

   # Extract tables and build a mapping of aliases to table names
    tables = parsed.find_all(Table)
    table_alias_map = {}
    for table in tables:
        table_name = table.name
        table_alias = table.alias or table_name  # Use table name if no alias

        # Extract catalog and schema
        table_catalog_expr = table.args.get("catalog")
        table_db_expr = table.args.get("db")

        # Extract catalog name as string
        catalog = table_catalog_expr.this if isinstance(table_catalog_expr, Identifier) else None

        # Extract schema name as string
        schema = table_db_expr.this if isinstance(table_db_expr, Identifier) else None

        # **Assign catalog and schema separately**
        metadata.tables.append(
            TableMetadata(
                name=table_name,
                alias=table.alias,
                catalog_name=catalog,  # Assign catalog name
                schema_name=schema,    # Assign schema name
            )
        )
        table_alias_map[table_alias] = table_name

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
                # For example, expressions without direct aliases
                alias_or_name = projection.alias_or_name if hasattr(projection, 'alias_or_name') else projection.sql()
                metadata.columns.append(
                    ColumnMetadata(
                        name=alias_or_name,
                        table=None,
                        alias=projection.alias,
                        expression=projection.sql(),
                    )
                )

    # Extract joins with detailed information
    joins = parsed.find_all(Join)
    for join in joins:
        join_type = join.kind.upper() if join.kind else "INNER"
        join_table_expr = join.args.get("this")
        
        # Determine right table details
        if isinstance(join_table_expr, Table):
            join_table = join_table_expr.name
            join_alias = join_table_expr.alias or join_table
        else:
            join_table = join_table_expr.sql()
            join_alias = None

        # Determine join condition
        join_condition_expr = join.args.get("on")
        join_condition = join_condition_expr.sql() if join_condition_expr else None

        # Attempt to infer left table from the join condition
        # This is a heuristic and may not be accurate for very complex queries
        left_table = "Unknown"
        left_alias = None
        right_table = join_table
        right_alias = join_alias

        if join_condition and "=" in join_condition:
            # Split the condition on '=' to identify table aliases
            left_side, right_side = join_condition.split("=", 1)
            left_alias_candidate = left_side.strip().split(".")[0]
            right_alias_candidate = right_side.strip().split(".")[0]
            left_table = table_alias_map.get(left_alias_candidate, left_alias_candidate)
            left_alias = left_alias_candidate
            # Update right_table and right_alias based on the condition
            right_table = table_alias_map.get(right_alias_candidate, right_alias_candidate)
            # If the join_table_expr has an alias, ensure it matches
            if join_alias:
                right_alias = join_alias
        else:
            # If no condition is found, default to the join table being the right table
            left_table = "Unknown"
            left_alias = None

        metadata.joins.append(
            JoinMetadata(
                type=join_type,
                left_table=left_table,
                right_table=join_table,
                left_alias=left_alias,
                right_alias=right_alias,
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

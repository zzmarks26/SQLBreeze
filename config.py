from typing import Set

SUPPORTED_DIALECTS: Set[str] = {
    "athena",
    "bigquery",
    "clickhouse",
    "databricks",
    "doris",
    "drill",
    "druid",
    "duckdb",
    "hive",
    "materialize",
    "mysql",
    "oracle",
    "postgres",
    "presto",
    "prql",
    "redshift",
    "risingwave",
    "snowflake",
    "spark",
    "spark2",
    "sqlite",
    "starrocks",
    "tableau",
    "teradata",
    "trino",
    "tsql",
}
from enum import Enum

class DialectEnum(str, Enum):
    athena = "athena"
    bigquery = "bigquery"
    clickhouse = "clickhouse"
    databricks = "databricks"
    doris = "doris"
    drill = "drill"
    druid = "druid"
    duckdb = "duckdb"
    hive = "hive"
    materialize = "materialize"
    mysql = "mysql"
    oracle = "oracle"
    postgres = "postgres"
    presto = "presto"
    prql = "prql"
    redshift = "redshift"
    risingwave = "risingwave"
    snowflake = "snowflake"
    spark = "spark"
    spark2 = "spark2"
    sqlite = "sqlite"
    starrocks = "starrocks"
    tableau = "tableau"
    teradata = "teradata"
    trino = "trino"
    tsql = "tsql"

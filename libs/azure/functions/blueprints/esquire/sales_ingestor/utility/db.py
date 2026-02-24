import os
from contextlib import contextmanager
from sqlalchemy import create_engine

_ENGINE = create_engine(
    os.environ["DATABIND_SQL_KEYSTONE"].replace("psycopg2", "psycopg"),      # postgresql+psycopg2://…
    pool_pre_ping=True, 
    pool_size=10,
    max_overflow=20, 
    future=True,
)

@contextmanager
def db():
    """Pooled AUTOCOMMIT connection (SQLAlchemy 2.x)."""
    with _ENGINE.begin() as conn:
        yield conn

def qtbl(table_name: str) -> str:
    """Adds the sales schema format to the table name."""
    return f'sales."{table_name}"'

PG_TYPES = {
    "string": "text",
    "large_string": "text",
    "int64": "bigint",
    "int32": "integer",
    "int16": "smallint",
    "int8":  "smallint",
    "uint64":"numeric",
    "float64":"double precision",
    "float32":"real",
    "bool":   "boolean",
}

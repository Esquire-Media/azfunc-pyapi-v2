from libs.data import register_binding
import os

register_binding(
    "keystone",
    "Structured",
    "sql",
    url=os.environ["DATABIND_SQL_KEYSTONE"],
    schemas=["keystone"],
    pool_size=1000,
    max_overflow=100,
)
register_binding(
    "audiences",
    "Structured",
    "sql",
    url=os.environ["DATABIND_SQL_AUDIENCES"],
    schemas=["dbo"],
    pool_size=1000,
    max_overflow=100,
)
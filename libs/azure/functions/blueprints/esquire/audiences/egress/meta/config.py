from libs.data import register_binding, from_bind
import os

if not from_bind("keystone"):
    register_binding(
        "keystone",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_KEYSTONE"],
        schemas={"keystone": None},
        pool_size=1000,
        max_overflow=100,
    )
if not from_bind("audiences"):
    register_binding(
        "audiences",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_AUDIENCES"],
        schemas={"dbo": None},
        pool_size=1000,
        max_overflow=100,
    )
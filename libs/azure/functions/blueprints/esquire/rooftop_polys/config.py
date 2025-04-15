from libs.data import register_binding, from_bind
import os

if not from_bind("postgres"):
    register_binding(
        "postgres",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_KEYSTONE"],
        schemas=["utils"],
    )

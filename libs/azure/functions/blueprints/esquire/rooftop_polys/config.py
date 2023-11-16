from libs.data import register_binding, from_bind
import os

if not from_bind("universal"):
    register_binding(
        "universal",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_UNIVERSAL"],
        schemas=["dbo"],
    )

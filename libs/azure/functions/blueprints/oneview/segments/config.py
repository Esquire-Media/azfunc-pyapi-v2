from libs.data import register_binding, from_bind
import os

# OneView
if not from_bind("roku"):
    register_binding(
        "roku",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_ROKU"],
        schemas=["dbo"],
    )
if not from_bind("general"):
    register_binding(
        "general",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_GENERAL"],
        schemas=["dbo"],
    )
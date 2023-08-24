from libs.data import register_binding, from_bind
import os

if not from_bind("onspot"):
    register_binding(
        "onspot",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_ONSPOT"],
        schemas=["dbo"],
)
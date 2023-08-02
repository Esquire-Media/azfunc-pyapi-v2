from libs.data import register_binding
import os

register_binding(
    "roku",
    "Structured",
    "sql",
    url=os.environ["DATABIND_SQL_ROKU"],
    schemas=["dbo"],
)

register_binding(
    "salesforce",
    "Structured",
    "sql",
    url=os.environ["DATABIND_SQL_SALESFORCE"],
    schemas=["dbo"],
)
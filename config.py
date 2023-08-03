from libs.data import register_binding
import os

register_binding(
    "onspot",
    "Structured",
    "sql",
    url=os.environ["DATABIND_SQL_ONSPOT"],
    schemas=["dbo"],
)

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

register_binding(
    "universal",
    "Structured",
    "sql",
    url=os.environ["DATABIND_SQL_UNIVERSAL"],
    schemas=["dbo"],
)
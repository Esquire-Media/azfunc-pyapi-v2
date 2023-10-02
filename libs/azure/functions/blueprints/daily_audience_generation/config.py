from libs.data import register_binding, from_bind
import os

if not from_bind("onspot"):
    register_binding(
        "onspot",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_ONSPOT"],
        schemas=["dbo"],
        pool_size=1000,
        max_overflow=100
    )

if not from_bind("audiences"):
    register_binding(
        "audiences",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_AUDIENCES"],
        schemas=["dbo"],
        pool_size=1000,
        max_overflow=100
    )

if not from_bind("salesforce"):
    register_binding(
        "salesforce",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_SALESFORCE"],
        schemas=["dbo"],
        pool_size=1000,
        max_overflow=100
    )

if not from_bind("universal"):
    register_binding(
        "universal",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_UNIVERSAL"],
        schemas=["dbo"],
    )
from libs.data import register_binding, from_bind
import os

if not from_bind("audiences"):
    register_binding(
        "audiences",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_AUDIENCES"],
        schemas=["dbo"],
)

if not from_bind("legacy"):
    register_binding(
        "legacy",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_UNIVERSAL"],
        schemas=["dbo","esquire"],
)
    
if not from_bind("foursquare"):
    register_binding(
        "foursquare",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_FOURSQUARE"],
        schemas=["dbo"],
)
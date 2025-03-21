from libs.data import register_binding, from_bind
import os

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
    
if not from_bind("keystone"):
    register_binding(
        "keystone",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_KEYSTONE"],
        schemas=["keystone"],
        pool_size=1000,
        max_overflow=100,
    )
    
if not from_bind("legacy"):
    register_binding(
        "legacy",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_UNIVERSAL"],
        schemas=["dbo","esquire"],
        pool_size=1000,
        max_overflow=100
)
    
if not from_bind("foursquare"):
    register_binding(
        "foursquare",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_FOURSQUARE"],
        schemas=["dbo"],
        pool_size=1000,
        max_overflow=100
)
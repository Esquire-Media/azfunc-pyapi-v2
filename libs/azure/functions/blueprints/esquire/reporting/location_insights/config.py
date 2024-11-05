from libs.data import register_binding, from_bind
import os

if not from_bind("keystone"):
    register_binding(
        "keystone",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_KEYSTONE"],
        schemas=["public"],
        pool_size=1000,
        max_overflow=100,
    )
    
if not from_bind("legacy"):
    register_binding(
        "legacy",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_UNIVERSAL"],
        schemas=["dbo"],
)
    
if not from_bind("synapse-general"):
    register_binding(
        "synapse-general",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_GENERAL"],
        schemas=["dbo"],
)
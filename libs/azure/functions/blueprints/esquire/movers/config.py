from libs.data import register_binding, from_bind
import os

if not from_bind('audiences'):
    register_binding(
        handle="audiences",
        protocol="Structured",
        scheme="sql",
        url=os.environ["DATABIND_SQL_AUDIENCES"],
        schemas=["dbo"]
    )
from libs.data import register_binding, from_bind
import os

if not from_bind("keystone"):
    register_binding(
        "keystone",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_KEYSTONE"],
        schemas=["keystone", "sales"],
        pool_size=1000,
        max_overflow=100,
    )
if not from_bind("general"):
    register_binding(
        "general",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_GENERAL"],
        schemas=["dbo"],
        pool_size=1000,
        max_overflow=100,
    )
if not from_bind("audiences"):
    register_binding(
        "audiences",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_AUDIENCES"],
        schemas=["dbo"],
        pool_size=1000,
        max_overflow=100,
    )

MAPPING_DATASOURCE = {
    # Deepsync mover - can use for testing
    "clwjn2q4s0056rw04ra44j8k9": {
        "dbType": "postgres",
        "bind": "keystone",
        "table": {
            "schema": "utils",
            "name": "movers",
        },
        "query": {
            "select": "add1 AS address, city, st AS state, zip AS \"zipCode\"",
            "filter": lambda length, unit: f" AND keycode >= NOW() - INTERVAL {length} {unit[0]}"
        },
    },
    # Esquire audiences
    "clwjn2q4r0053rw04l2rscs07": {
        "dbType": "postgres",
        "bind": "keystone",
        "table": {
            "schema": "keystone",
            "name": "Audience",
        },
    },
    # Esquire geoframes - will need to change
    "clwjn2q4r0054rw04f76se61o": {
        "dbType": "postgres",
        "bind": "keystone",
        "table": {
            "schema": "keystone",
            "name": "TargetingGeoFrame",
        },
    },
    # Esquire sales - not ready to be used yet
    "clwjn2q4t0057rw04kbhlog0s": {
        "dbType": "postgres",
        "bind": "keystone",
        "table": {
            "schema": "sales",
            "name": "entities",
        },
        "isEAV":True
    },
}

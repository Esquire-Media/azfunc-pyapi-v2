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
if not from_bind("foursquare"):
    register_binding(
        "foursquare",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_FOURSQUARE"],
        schemas=["dbo"],
        pool_size=1000,
        max_overflow=100,
    )

MAPPING_DATASOURCE = {
    # Attom estated data - can use for testing
    "clwjn2q4s0055rw04ojmpvg77": {
        "dbType": "synapse",
        "bind": "audience",
        "table": {
            "schema": "dbo",
            "name": "addresses",
        },
    },
    # Deepsync mover - can use for testing
    "clwjn2q4s0056rw04ra44j8k9": {
        "dbType": "synapse",
        "bind": "audiences",
        "table": {
            "schema": "dbo",
            "name": "movers",
        },
        "query": {
            "select": "address, city, state, zipcode as zipCode",
            "filter": lambda length, unit: " AND CONVERT(DATE, [date], 126) >= DATEADD({}, {}, GETDATE())".format(
                unit[0], 0 - length
            ),
        },
    },
    # Esquire audiences
    "clwjn2q4r0053rw04l2rscs07": {
        "dbType": "postgres",
        "bind": "keystone",
        "table": {
            "schema": "public",
            "name": "Audience",
        },
    },
    # Esquire geoframes - will need to change
    "clwjn2q4r0054rw04f76se61o": {
        "dbType": "postgres",
        "bind": "keystone",
        "table": {
            "schema": "public",
            "name": "TargetingGeoFrame",
        },
    },
    # Esquire sales - not ready to be used yet
    "clwjn2q4t0057rw04kbhlog0s": {
        "dbType": "synapse",
        "bind": "general",
        "table": {
            "schema": "dbo",
            "name": "sales",
        },
    },
    # Foursquare POI - can use for testing
    "clwjn2q4t0058rw04fx6qanbh": {
        "dbType": "synapse",
        "bind": "foursquare",
        "table": {
            "schema": "dbo",
            "name": "poi",
        },
    },
    # OSM Building footprints - not set up in Synapse
    "clwjn2q4t0059rw04qxcw5q3h": {
        "dbType": "synapse",
        "bind": "general",
        "table": {
            "schema": "dbo",
            "name": "osm",
        },
    },
}

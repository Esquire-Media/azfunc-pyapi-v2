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
if not from_bind("universal"):
    register_binding(
        "universal",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_UNIVERSAL"],
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
    "clujtgezq0017nh4dqcygpyap": {
        "bind": "audience",
        "table": {
            "schema": "dbo",
            "name": "addresses",
        },
    },
    # Deepsync mover - can use for testing
    "clujtgf3m0018nh4dri1bl9aw": {
        "bind": "audiences",
        "table": {
            "schema": "dbo",
            "name": "movers",
        },
    },
    # Esquire audiences
    "clujtgf5i0019nh4dt45yitg0": {
        "bind": "keystone",
        "table": {
            "schema": "public",
            "name": "Audience",
        },
    },
    # Esquire geoframes - will need to change
    "clujtgf79001anh4dt37ktpjf": {
        "bind": "universal",
        "table": {
            "schema": "dbo",
            "name": "Locations",
        },
    },
    # Esquire sales - not ready to be used yet
    "clujtgf8v001bnh4dn2zqvi8a": {
        "bind": "general",
        "table": {
            "schema": "dbo",
            "name": "sales",
        },
    },
    # Foursquare POI - can use for testing
    "clujtgfan001cnh4dg5jn4epm": {
        "bind": "foursquare",
        "table": {
            "schema": "dbo",
            "name": "poi",
        },
    },
    # OSM Building footprints - not set up in Synapse
    "clujtgfcf001dnh4dsumr0fkv": {
        "bind": "general",
        "table": {
            "schema": "dbo",
            "name": "osm",
        },
    },
}

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
    "clulpbbi2001d12jiy1zvwdhy": {
        "bind": "audience",
        "table": {
            "schema": "dbo",
            "name": "addresses",
        },
    },
    # Deepsync mover - can use for testing
    "clulpbbl8001e12jii8y3k8gb": {
        "bind": "audiences",
        "table": {
            "schema": "dbo",
            "name": "movers",
        },
    },
    # Esquire audiences
    "clulpbbn3001f12jiptemet9v": {
        "bind": "keystone",
        "table": {
            "schema": "public",
            "name": "Audience",
        },
    },
    # Esquire geoframes - will need to change
    "clulpbbon001g12ji1b939rbs": {
        "bind": "universal",
        "table": {
            "schema": "dbo",
            "name": "Locations",
        },
    },
    # Esquire sales - not ready to be used yet
    "clulpbbqg001h12jispdl3car": {
        "bind": "general",
        "table": {
            "schema": "dbo",
            "name": "sales",
        },
    },
    # Foursquare POI - can use for testing
    "clulpbbsi001i12jik2oojymu": {
        "bind": "foursquare",
        "table": {
            "schema": "dbo",
            "name": "poi",
        },
    },
    # OSM Building footprints - not set up in Synapse
    "clulpbbuc001j12ji27yfr796": {
        "bind": "general",
        "table": {
            "schema": "dbo",
            "name": "osm",
        },
    },
}

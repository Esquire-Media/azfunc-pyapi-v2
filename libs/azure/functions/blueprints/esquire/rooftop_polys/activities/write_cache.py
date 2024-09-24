# File: libs/azure/functions/blueprints/esquire/audiences/daily_audience_generation/activities/write_cache.py

from azure.durable_functions import Blueprint
from datetime import datetime as dt
from shapely.geometry import shape
from sqlalchemy import create_engine
from typing import List
import os

bp: Blueprint = Blueprint()


# activity to validate the addresses
@bp.activity_trigger(input_name="polys")
def activity_rooftopPolys_writeCache(polys: List[dict]):
    pg_engine = create_engine(os.environ["DATABIND_SQL_POSTGRES"])

    # Define the upsert query (for WKB insertion)
    upsert_query = """
    INSERT INTO public.google_rooftop_cache (query, boundary, last_updated) 
    VALUES (%s, ST_GeomFromText(%s), %s)
    ON CONFLICT (query) 
    DO UPDATE SET
    boundary = EXCLUDED.boundary,
    last_updated = EXCLUDED.last_updated;
    """

    # Establish a connection to PostgreSQL
    conn = pg_engine.raw_connection()
    cursor = conn.cursor()

    # Loop through the DataFrame and decode + re-encode WKB
    for p in polys:
        cursor.execute(upsert_query, (p["query"], shape(p["geojson"]).wkt, dt.utcnow()))

    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()

    return {}

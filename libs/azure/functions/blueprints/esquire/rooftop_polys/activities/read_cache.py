# File: libs/azure/functions/blueprints/esquire/rooftop_polys/activities/read_cache.py

from azure.durable_functions import Blueprint
from sqlalchemy import create_engine, text
from typing import List
import pandas as pd, os, shapely, geojson

bp: Blueprint = Blueprint()

# chunk_size = int(os.environ["chunk_size"])
max_sql_parameters = 1000
# maximum number of parameters that MS SQL can parse is ~2100


# activity to validate the addresses
@bp.activity_trigger(input_name="addresses")
def activity_rooftopPolys_readCache(addresses: List[str]):
    if not addresses:
        return {}

    pg_engine = create_engine(os.environ["DATABIND_SQL_KEYSTONE"])
    try:
        with pg_engine.connect() as conn:
            records = conn.execute(
                text("""
                    SELECT query, boundary
                    FROM utils.google_rooftop_cache
                    WHERE query = ANY(:addresses)
                """),
                {"addresses": addresses},
            ).fetchall()
    finally:
        pg_engine.dispose()

    df = pd.DataFrame(records, columns=["query", "boundary"])
    if not df.empty:
        df["boundary"] = df["boundary"].apply(lambda x: geojson.dumps(shapely.geometry.mapping(shapely.from_wkb(x))))
        return df[["query", "boundary"]].to_dict()
    return {}

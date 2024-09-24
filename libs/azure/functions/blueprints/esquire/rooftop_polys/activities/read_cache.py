# File: libs/azure/functions/blueprints/esquire/audiences/daily_audience_generation/activities/read_cache.py

from azure.durable_functions import Blueprint
from sqlalchemy import create_engine
from typing import List
import pandas as pd, os, shapely, geojson

bp: Blueprint = Blueprint()

# chunk_size = int(os.environ["chunk_size"])
max_sql_parameters = 1000
# maximum number of parameters that MS SQL can parse is ~2100


# activity to validate the addresses
@bp.activity_trigger(input_name="addresses")
def activity_rooftopPolys_readCache(addresses: List[str]):
    pg_engine = create_engine(os.environ["DATABIND_SQL_POSTGRES"])
    read_query = f"""
        SELECT
            query
            ,boundary 
        FROM public.google_rooftop_cache
        WHERE query IN ({",".join(map(lambda a: f"'{a}'", addresses))})
    """
    df = pd.read_sql(read_query, pg_engine)

    if not df.empty:
        df["boundary"] = df["boundary"].apply(lambda x: geojson.dumps(shapely.geometry.mapping(shapely.from_wkb(x))))
        return df[["query", "boundary"]].to_dict()
    return {}

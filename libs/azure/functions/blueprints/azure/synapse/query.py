# File: libs/azure/functions/blueprints/synapse/query.py

from libs.azure.functions import Blueprint
from libs.data import from_bind
import pandas as pd

try:
    import orjson as json
except:
    import json

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
async def activity_synapse_query(ingress: dict):
    """
    This function counts the records in a given query.

    Params
    bind    Bind that specifies the Synapse database to connect to.
    query   SQL query (e.g. "OPENROWSET(...) AS [data]")
    """

    df = pd.read_sql(
        ingress["query"], from_bind(ingress["bind"]).connect().connection()
    )
    json_data = df.to_json(orient="records")
    return json.loads(json_data)

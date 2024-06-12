# File: libs/azure/functions/blueprints/esquire/audiences/daily_audience_generation/activities/read_cache.py

from azure.durable_functions import Blueprint
import os
from typing import List
from sqlalchemy.orm import Session
from libs.data import from_bind
import pandas as pd

bp: Blueprint = Blueprint()

chunk_size = int(os.environ["chunk_size"])
max_sql_parameters = 1000
# maximum number of parameters that MS SQL can parse is ~2100


# activity to validate the addresses
@bp.activity_trigger(input_name="addresses")
def activity_rooftopPolys_readCache(addresses: List[str]):
    """
    Read from the SQL GoogleRooftopCache to check for cached frames among the passed addresses.
    """
    # set the provider
    provider = from_bind("universal")
    rooftop = provider.models["dbo"]["GoogleRooftopCache"]
    session: Session = provider.connect()

    # get df for each address and add the poly to the addresses information
    df = pd.DataFrame(
        session.query(rooftop.Query, rooftop.Boundary, rooftop.LastUpdated)
        .filter(rooftop.Query.in_(addresses))
        .order_by(rooftop.Query, rooftop.LastUpdated.desc())
    )
    
    if not df.empty:
        df.drop_duplicates(subset="Query", keep="first", inplace=True)
        return df[["Query", "Boundary"]].to_dict()
    return {}

# File: libs/azure/functions/blueprints/daily_audience_generation/activities/write_cache.py

from libs.azure.functions import Blueprint
import os
from typing import List
from sqlalchemy.orm import Session
from libs.data import from_bind
import pandas as pd
from datetime import datetime as dt
import logging
import json

bp: Blueprint = Blueprint()

chunk_size = int(os.environ["chunk_size"])
max_sql_parameters = 1000
# maximum number of parameters that MS SQL can parse is ~2100


# activity to validate the addresses
@bp.activity_trigger(input_name="addresses")
def activity_write_cache(addresses: List[str]):
    # set the provider
    provider = from_bind("sisense-etl")
    rooftop = provider.models["dbo"]["GoogleRooftopCache"]
    session: Session = provider.connect()

    data = json.loads(addresses)
    # NOTE: pd.DataFrame() ['query','geojson']
    logging.warning(data)
    
    # iteratively create a list of new frames to add to the cache
    adds = []
    for entry in data:
        # logging.warning(entry['query'])
        # logging.warning(entry['geojson'])
        new_frame = rooftop(
            Query=entry['query'],
            Boundary=str({'type': 'Feature', 'geometry': entry['geojson']}), 
            LastUpdated=dt.utcnow()
        )
        adds.append(new_frame)
    logging.warning(dt.utcnow())
    session.add_all(adds)
    session.commit()

    # SELECT TOP (1000) [Query], [Boundary], [LastUpdated]
    # FROM [dbo].[GoogleRooftopCache]
    # WHERE [Query] = '1512 OURAY AVE, FORT MORGAN CO, 80701';
    
    return {}
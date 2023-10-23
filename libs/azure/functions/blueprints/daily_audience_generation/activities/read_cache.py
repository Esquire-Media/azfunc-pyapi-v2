# File: libs/azure/functions/blueprints/daily_audience_generation/activities/read_cache.py

from libs.azure.functions import Blueprint
import os
from sqlalchemy.orm import Session
from libs.data import from_bind
import pandas as pd
from datetime import datetime
import logging

bp: Blueprint = Blueprint()

chunk_size = int(os.environ["chunk_size"])
max_sql_parameters = 1000 
# maximum number of parameters that MS SQL can parse is ~2100

# activity to validate the addresses
@bp.activity_trigger(input_name="ingress")
def activity_read_cache(ingress: dict):
    """
    Read from the SQL GoogleRooftopCache to check for cached frames among the passed addresses.
    """
    # {
    #     "path": "raw/f197df628ee04857b2030fd8901ba934/audiences",
    #     "instance_id": "f197df628ee04857b2030fd8901ba934",
    #     "context": None,
    #     "a0H6e00000bNazEEAS_test": '[{"query_string":"105 FILLMORE ST, DENVER CO, 80206"},{"query_string":"1837 PRIMROSE PL, ERIE CO, 80516"},{"query_string":"412 WASHINGTON AVE, BRECKENRIDGE CO, 80424"}]',
    # }
    # logging.warning(f"reach cache Ingress: {ingress}")
    # the below works as intended and pulls the needed information from ingress
    # logging.warning(ingress['audience_id'])
    # logging.warning(ingress['addresses'])
    # logging.warning(ingress)
    
    # set the provider
    provider = from_bind('sisense-etl')
    rooftop = provider.models["dbo"]["GoogleRooftopCache"]
    session: Session = provider.connect()
    
    df = pd.DataFrame(
        session.query(
            rooftop.Query,
            rooftop.Boundary,
            rooftop.LastUpdated
        ).filter(rooftop.Query == "1 ACADEMY LN, OLD LYME CT, 06371")
    )
    
    # get today's date 
    today = datetime.now()
    
    # Calculate the absolute differences between dates and today's date
    df['DateDiff'] = (df['LastUpdated'] - today).abs()
    
    # Find the index of the row with the closest date to today's date
    closest_row_index = df['DateDiff'].idxmin()

    # Select the row with the closest date
    closest_row = df.loc[closest_row_index]
    
    logging.warning(df)
    
    logging.warning("____")
    logging.warning(closest_row['Boundary'])
    
    # query the db for rooftop polys, if the exsist
    # query = f"""
    #     SELECT
    #         [Query],[Boundary],[LastUpdated]
    #     FROM [dbo].[GoogleRooftopCache]
    #     WHERE [Query] = '1 ACADEMY LN, OLD LYME CT, 06371'
    #     ;"""
    # # connection to db
    # conn = os.environ["DATABIND_SQL_ROOFTOPS"]
    # df = pd.read_sql(query, conn)
    
    # logging.warning(df)

    return {}

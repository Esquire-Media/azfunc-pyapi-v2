# File: libs/azure/functions/blueprints/daily_audience_generation/activities/read_cache.py

from libs.azure.functions import Blueprint
import os
from sqlalchemy.orm import Session
from libs.data import from_bind
import pandas as pd
from datetime import datetime
import json
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
    # logging.warning(f"Read Cache Ingress: {ingress}")
    # set the provider
    provider = from_bind("sisense-etl")
    rooftop = provider.models["dbo"]["GoogleRooftopCache"]
    session: Session = provider.connect()

    # list of address information
    addresses_with_poly = []
    addresses_without_poly = []

    for address in json.loads(ingress["addresses"]):
        # get df for each address and add the poly to the addresses information
        df = pd.DataFrame(
            session.query(rooftop.Query, rooftop.Boundary, rooftop.LastUpdated)
            .filter(rooftop.Query == address["query_string"])
            .order_by(rooftop.LastUpdated)
        )
        # if the dataframe is empty
        if df.empty:
            # append list with no polys
            addresses_without_poly.append(address)
        else:
            # Get today's date
            today = datetime.now()

            # Calculate the absolute differences between dates and today's date
            df["DateDiff"] = (df["LastUpdated"] - today).abs()

            # Find the index of the row with the closest date to today's date
            closest_row_index = df["DateDiff"].idxmin()

            # Select the row with the closest date and add it to the address'
            address["poly"] = df.loc[closest_row_index]["Boundary"]

            # append the address information to the addresses list
            addresses_with_poly.append(address)

    audience = {
        "audience_id": ingress["audience_id"],
        "addresses": addresses_with_poly,
        "addresses_no_poly": addresses_without_poly,
    }

    return audience

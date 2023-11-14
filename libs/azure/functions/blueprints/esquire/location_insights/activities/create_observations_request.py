from libs.azure.functions import Blueprint
import pandas as pd
import json
import os
from azure.storage.blob import BlobClient
from datetime import datetime as dt, timedelta
from libs.azure.storage.blob.sas import get_blob_download_url
from libs.utils.time import get_local_timezone, local_time_to_utc

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()


# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_locationInsights_createObservationsRequest(settings: dict):
    
    # calculate local start and end datetimes in UTC time
    start_date_local = dt.fromisoformat(settings["endDate"]) - timedelta(
        days=111
    )  # 16 weeks minus one day
    end_date_local = (
        dt.fromisoformat(settings["endDate"]) + timedelta(days=1) - timedelta(seconds=1)
    )  # EOD on the endDate specified

    # load the locations blob into Pandas using SAS
    blob_client = BlobClient.from_connection_string(
        conn_str=os.environ[settings["runtime_container"]["conn_str"]],
        container_name=settings["runtime_container"]["container_name"],
        blob_name=settings["runtime_container"]["location_blob"],
    )
    locations = pd.read_csv(get_blob_download_url(blob_client=blob_client))
    loc = locations.iloc[0]

    # build a feature collection with the fields required for Onspot observations request

    # determine the timezone where this location exists
    local_timezone = get_local_timezone(
        latitude=loc["Latitude"], longitude=loc["Longitude"]
    )

    # build the feature payload
    feature_collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": json.loads(loc["Geometry"]),
                "properties": {
                    "hash": False,
                    "fileFormat": {"delimiter": ",", "quoteEncapsulate": True},
                    "name": f"{loc['ESQ_ID']}",
                    "fileName": f"{loc['ESQ_ID']}",
                    "start": local_time_to_utc(  # calculate UTC times based on local timezone
                        local_time=start_date_local, local_timezone=local_timezone
                    )
                    .isoformat()
                    .split("+")[0],
                    "end": local_time_to_utc(
                        local_time=end_date_local, local_timezone=local_timezone
                    )
                    .isoformat()
                    .split("+")[0],
                },
            }
        ],
    }

    return feature_collection
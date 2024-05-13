#  file path:libs/azure/functions/blueprints/esquire/audiences/meta/activities/get_facebook_updateDeviceids.py

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.customaudience import CustomAudience
from facebook_business.exceptions import FacebookRequestError
from libs.azure.functions import Blueprint
from azure.storage.blob import (
    BlobClient,
)
from facebook_business.api import FacebookAdsApi
from libs.azure.functions import Blueprint
import os,logging
from io import BytesIO
import pandas as pd
import hashlib

bp: Blueprint = Blueprint()


# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
async def activity_esquireAudiencesMeta_facebookUpdateAudience(ingress: dict):
    # Initialize the Facebook API
    access_token = os.environ["META_ACCESS_TOKEN"]
    FacebookAdsApi.init(access_token=access_token)

    logging.warning(f"Ingress: {ingress}")

    # get the device IDs
    blob = BlobClient.from_connection_string(
        conn_str=os.environ["ESQUIRE_AUDIENCE_CONN_STR"],
        container_name="general",
        blob_name="audiences/a0H5A00000aZbI1UAK/2023-12-04T18:16:59.757249+00:00/maids.csv",
    )
    df = pd.read_csv(BytesIO(blob.download_blob().readall()), header=None)
    df.columns = ["DeviceID"]

    short_maid_list = df["DeviceID"].head(100).tolist()
    logging.warning(short_maid_list)

    # Custom Audience ID and Ad Account ID
    custom_audience_id = ingress["audience_id"]
    try:
        audience = CustomAudience(custom_audience_id)
        result = audience.add_users(
            schema="MOBILE_ADVERTISER_ID",
            users=[hash_maid(maid) for maid in short_maid_list],
        )  # Add new users

        logging.warning(result)
        return result

    except FacebookRequestError as e:
        return f"An error occurred: {e}"

# function to has the MAID values passed into Facebook audience
def hash_maid(maid):
    return hashlib.sha256(maid.encode("utf-8")).hexdigest()

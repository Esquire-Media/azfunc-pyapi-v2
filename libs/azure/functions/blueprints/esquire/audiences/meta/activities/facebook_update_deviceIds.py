#  file path:libs/azure/functions/blueprints/esquire/audiences/meta/activities/get_facebook_audience.py

from azure.storage.blob.aio import ContainerClient
from datetime import datetime
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.customaudience import CustomAudience
from facebook_business.exceptions import FacebookRequestError
from libs.azure.functions import Blueprint
import os
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    BlobServiceClient,
    ContainerClient,
    ContainerSasPermissions,
    generate_blob_sas,
    generate_container_sas,
)
from dateutil.relativedelta import relativedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from libs.azure.functions import Blueprint
import os, datetime, logging, json
from io import BytesIO
import pandas as pd
from urllib.parse import unquote

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
    df.columns = ['DeviceID']
    
    short_dev_id_list = df['DeviceID'].tolist().head(100)
    logging.warning(short_dev_id_list)
    
    # pass the devices IDs into the 
    
    return {}

    # # Custom Audience ID and Ad Account ID
    # custom_audience_id = ingress['audience']
    # try:
    #     audience = CustomAudience(custom_audience_id)
    #     payload = {
    #         'payload': {
    #             'schema': 'MOBILE_ADVERTISER_ID',  
    #             'data': device_ids
    #         },
    #         'pre_hashed': True  # set to True if your device IDs are pre-hashed, False otherwise
    #     }
        # might have to do different payloads for the different ID types?
        
        # # example loop
        # if device_type == "IDFA":
        #     payload = {
        #         'payload':{
        #             'schema':'IDFA',
        #             'data': [
        #                 'hashed-idfa-1',
        #                 'hashed-idfa-2',
        #             ]
        #         }
        #     }
        # elif device_type == "ANDROID_ID":
        #     payload = {
        #         'payload':{
        #             'schema':'ANDROID_ID',
        #             'data': [
        #                 'hashed-ANDROID_ID-1',
        #                 'hashed-ANDROID_ID-2',
        #             ]
        #         }
        #     }
            
        
    #     result = audience.delete()
    #     result = audience.add_users(payload)
        
    #     return f"Successfully performed replace operation on custom audience.", result
    # except FacebookRequestError as e:
    #     return f"An error occurred: {e}"
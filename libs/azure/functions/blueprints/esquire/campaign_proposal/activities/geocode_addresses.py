import os
from libs.azure.functions import Blueprint
import pandas as pd
from libs.utils.smarty import bulk_validate
from azure.storage.blob import BlobServiceClient, ContainerClient

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_campaignProposal_geocodeAddresses(settings: dict):

    # two formats for addresses are accepted - geocoded and non-geocoded. 
    # here we separate the inputs into two buckets based on whether they still need to be sent to smarty.
    addresses = pd.DataFrame(settings["addresses"])
    if 'latitude' and 'longitude' in addresses.columns:
        pre_geocoded = addresses[(~addresses['latitude'].isnull())&(~addresses['longitude'].isnull())]
        to_geocode = addresses[(addresses['latitude'].isnull())|(addresses['longitude'].isnull())]
    else:
        pre_geocoded = pd.DataFrame()
        to_geocode = addresses
    
    if len(to_geocode):
        # clean and geocode addresses using Smarty. Retain only [address, lat, long] columns
        cleaned = bulk_validate(
            to_geocode,
            address_col="address",
            city_col="city",
            state_col="state",
            zip_col="zip",
        )[[
            "delivery_line_1",
            "latitude",
            "longitude",
        ]].rename(columns={
            "delivery_line_1": "address",
            "latitude": "latitude",
            "longitude": "longitude",
        })
    else:
        cleaned = pd.DataFrame()

    # combine pre-geocoded and newly-geocoded addresses into one output dataframe
    if len(pre_geocoded) and len(cleaned):
        output = pd.concat([pre_geocoded, cleaned])
    elif len(pre_geocoded) and not len(cleaned):
        output = pre_geocoded
    elif not len(pre_geocoded) and len(cleaned):
        output = cleaned

    # return the validated addresses as a list of component dictionaries, each with an index attribute
    container_client: ContainerClient = BlobServiceClient.from_connection_string(os.environ["ESQUIRE_REPORTS_CONN_STR"]).get_container_client(container="campaign-proposals")
    blob_client = container_client.get_blob_client(blob=f"{settings['instance_id']}/addresses.csv")
    blob_client.upload_blob(data=output.to_csv(index=False))

    return {}
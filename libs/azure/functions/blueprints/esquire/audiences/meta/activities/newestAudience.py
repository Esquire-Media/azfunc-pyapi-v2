#  file path:libs/azure/functions/blueprints/esquire/audiences/meta/activities/newestAudience.py

from azure.storage.blob.aio import ContainerClient
from azure.storage.blob import (
    BlobClient,
    BlobServiceClient,
    DelimitedTextDialect,
    BlobSasPermissions,
    generate_blob_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
import os

bp: Blueprint = Blueprint()


# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
async def activity_esquireAudiencesMeta_newestAudience(ingress: dict):
    # ingress = {
    #     "conn_str": os.environ["ESQUIRE_AUDIENCE_CONN_STR"],
    #     "container_name": "general",
    #     "audience_id": ingress,
    # }

    # get path to most recent blob
    blob_service_client = BlobServiceClient.from_connection_string(ingress["conn_str"])
    container_client = blob_service_client.get_container_client(
        ingress["container_name"]
    )

    # get list of blobs
    blob_list = []
    for blob in container_client.list_blobs(
        name_starts_with=f"audiences/{ingress['audience_id']}/"
    ):
        blob_list.append(blob.name)

    for path in blob_list:
        dt = extract_datetime(path)

    most_recent_datetime = None

    if most_recent_datetime is None or dt > most_recent_datetime:
        most_recent_datetime = dt
        most_recent_path = path

    return most_recent_path[: most_recent_path.rfind("/") + 1]


# Function to extract the datetime from the path
def extract_datetime(path):
    # Split the path and get the datetime part
    parts = path.split("/")
    datetime_str = parts[2]  # Adjust the index based on your path structure
    return datetime.fromisoformat(datetime_str)

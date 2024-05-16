#  file path:libs/azure/functions/blueprints/esquire/audiences/xandr/activities/getTotalMaids.py

from libs.azure.functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    BlobServiceClient, 
    DelimitedTextDialect,
    BlobSasPermissions,
    generate_blob_sas,
)
from libs.azure.functions import Blueprint
from io import BytesIO
import logging, os, json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

bp: Blueprint = Blueprint()


# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
async def activity_esquireAudiencesXandr_getTotalMaids(ingress: dict):
    # get path to most recent blob
    connect_str = ingress["conn_str"]
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = ingress["container_name"]
    container_client = blob_service_client.get_container_client(container_name)
    
    # get list of blobs
    blob_list = []
    for blob in container_client.list_blobs(name_starts_with=f"audiences/{ingress['audience_id']}"):
        blob_list.append(blob.name)
        
    for path in blob_list:
        dt = extract_datetime(path)
    if most_recent_datetime is None or dt > most_recent_datetime:
        most_recent_datetime = dt
        most_recent_path = path

    # get list of blobs
    
    # set blob information
    blob = BlobClient.from_connection_string(
        conn_str=ingress["conn_str"],
        container_name=ingress["container_name"],
        blob_name=ingress["blob_name"],
    )
    # Define the dialect for the CSV format (assumes default comma delimiters)
    dialect = DelimitedTextDialect(
        delimiter=",",  # Specify the delimiter, e.g., comma, semicolon, etc.
        quotechar='"',  # Character used to quote fields
        lineterminator="\n",  # Character used to separate records
        has_header="true",  # Use 'true' if the CSV has a header row, otherwise 'false'
    )

    # get maids count
    total_maids = blob.query_blob(
        "SELECT COUNT() FROM BlobStorage",
        blob_format=dialect,
        output_format=dialect,
    )
    # df = pd.read_csv(BytesIO(blob.download_blob().readall()), header=None)
    # total_maids = len(df)

    # get the blob_url
    blob_url_with_sas = (
        blob.url
        + "?"
        + generate_blob_sas(
            account_name=blob.account_name,
            container_name=blob.container_name,
            blob_name=blob.blob_name,
            account_key=blob.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
    )

    return (blob_url_with_sas, int(total_maids))

# Function to extract the datetime from the path
def extract_datetime(path):
    # Split the path and get the datetime part
    parts = path.split('/')
    datetime_str = parts[2]  # Adjust the index based on your path structure
    return datetime.fromisoformat(datetime_str)
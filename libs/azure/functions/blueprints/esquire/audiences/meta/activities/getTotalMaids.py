#  file path:libs/azure/functions/blueprints/esquire/audiences/meta/activities/getTotalMaids.py

from libs.azure.functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    BlobServiceClient,
    DelimitedTextDialect,
    BlobSasPermissions,
    generate_blob_sas,
)
from memory_profiler import profile
from libs.azure.functions import Blueprint
from io import BytesIO
import logging, os, json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

bp: Blueprint = Blueprint()


# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
async def activity_esquireAudiencesMeta_getTotalMaids(ingress: dict):   
    # ingress = {
    #     "conn_str": os.environ["ESQUIRE_AUDIENCE_CONN_STR"],
    #     "container_name": "general",
    #     "path_to_blobs": blob_path,
    #     "audience_id": ingress,
    # }
    
    blob_service_client = BlobServiceClient.from_connection_string(ingress['conn_str'])
    container_client = blob_service_client.get_container_client(ingress['container_name'])
    
    result = {
        "count": 0 
    }
    blob_count = 0
    # get list of all blobs in the given folder
    for blob in container_client.list_blobs(name_starts_with=ingress['path_to_blobs']):
        blob_count +=1 
        # set blob information
        blob = BlobClient.from_connection_string(
            conn_str=ingress["conn_str"],
            container_name=ingress["container_name"],
            blob_name=blob.name,
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
        
        result['count'] = blob_count
        result[f'Blob_{blob_count}'] = {
            'url': blob_url_with_sas,
            'maids_count': total_maids
        }

    return result

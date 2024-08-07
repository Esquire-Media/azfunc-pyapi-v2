#  file path:libs/azure/functions/blueprints/esquire/audiences/utils/activities/getTotalMaids.py

from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    ContainerClient,
    DelimitedTextDialect,
    BlobSasPermissions,
    generate_blob_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

bp: Blueprint = Blueprint()


# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
async def activity_esquireAudiencesUtils_getTotalMaids(ingress: dict):
    # ingress = {
    #     "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
    #     "container_name": "general",
    #     "path_to_blobs": blob_path,
    #     "audience_id": ingress,
    # }

    container_client = ContainerClient.from_connection_string(
        conn_str=os.environ.get(ingress["conn_str"], ingress["conn_str"]),
        container_name=ingress["container_name"],
    )

    result = []
    # get list of all blobs in the given folder
    for blob in container_client.list_blobs(name_starts_with=ingress["blob_prefix"]):
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
            "SELECT COUNT(*) FROM BlobStorage",
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

        result.append(
            {
                "url_with_sas": blob_url_with_sas,
                "count": int(total_maids.readall().decode("utf-8").strip()),
            }
        )

    return result

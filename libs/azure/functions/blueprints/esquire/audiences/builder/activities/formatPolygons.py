# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/formatPolygons.py

from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import unquote
import os, pandas as pd, orjson as json

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_formatPolygons(ingress: dict):
    """
    Formats polygon data from a CSV file into a JSON format and uploads it to a destination blob.

    This activity reads polygon data from a source blob, processes it to extract features, converts the data into JSON format, and uploads the JSON data to a destination blob.

    Parameters:
    ingress (dict): A dictionary containing source and destination blob information.
        {
            "source": str,
            "destination": dict
        }

    Returns:
    str: The URL of the destination blob with a SAS token for read access.

    Raises:
    Exception: If an error occurs during the blob operations.
    """
    # Initialize the source BlobClient
    input_blob = BlobClient.from_blob_url(ingress["source"])
    df = pd.read_csv(ingress["source"])

    # Initialize the destination BlobClient
    output_blob = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["destination"]["conn_str"]],
        container_name=input_blob.container_name,
        blob_name=input_blob.blob_name + ".json",
    )

    # Process the CSV data and convert to JSON format
    output_blob.upload_blob(
        json.dumps(
            [
                feature
                for item in df["polygon"].apply(eval)
                for feature in item.get("features", [])
            ]
        ),
        overwrite=True
    )

    # Generate a SAS token for the destination blob with read permissions
    sas_token = generate_blob_sas(
        account_name=output_blob.account_name,
        container_name=output_blob.container_name,
        blob_name=output_blob.blob_name,
        account_key=output_blob.credential.account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + relativedelta(days=2),
    )

    # Return the URL of the destination blob with the SAS token
    return unquote(output_blob.url) + "?" + sas_token

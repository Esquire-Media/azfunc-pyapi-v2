# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/formatOnspotRequest.py

from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    extract_dates,
)
from urllib.parse import unquote
import os, uuid, orjson as json

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_formatOnspotRequest(ingress: dict):
    """
    Formats a request for Onspot processing from polygon data and uploads it to a destination blob.

    This activity reads polygon data from a source blob, processes it to extract features, adds metadata to each feature, and uploads the formatted request to a destination blob.

    Parameters:
    ingress (dict): A dictionary containing source and destination blob information and custom coding for request.
        {
            "source_url": str,
            "working": {
                "conn_str": str,
                "container_name": str,
                "blob_prefix": str,
            },
            "custom_coding": {
                "request": dict
            }
        }

    Returns:
    str: The URL of the destination blob with a SAS token for read access.

    Raises:
    Exception: If an error occurs during the blob operations.
    """
    now = datetime.utcnow()
    if ingress.get("custom_coding", {}).get("request", {}):
        start, end = extract_dates(ingress["custom_coding"]["request"], now)
    else:
        start = now - relativedelta(days=33)
        end = now - relativedelta(days=2)

    # Initialize the source BlobClient
    input_blob = BlobClient.from_blob_url(ingress["source_url"])
    features = json.loads(input_blob.download_blob().readall())

    # Create the request payload with features and metadata
    request = {
        "type": "FeatureCollection",
        "features": [
            {
                **feature,
                "properties": {
                    "name": uuid.uuid4().hex,
                    "fileName": uuid.uuid4().hex + ".csv",
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "hash": False,
                    "fileFormat": {
                        "delimiter": ",",
                        "quoteEncapsulate": True,
                    },
                },
            }
            for feature in features
        ],
    }

    # Initialize the destination BlobClient
    output_blob = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["working"]["conn_str"]],
        container_name=ingress["working"]["container_name"],
        blob_name="{}/{}.json".format(
            ingress["working"]["blob_prefix"], uuid.uuid4().hex
        ),
    )
    output_blob.upload_blob(json.dumps(request))

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

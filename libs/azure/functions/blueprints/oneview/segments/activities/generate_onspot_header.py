# File: libs/azure/functions/blueprints/oneview/segments/activities/generate_onspot_header.py

from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from urllib.parse import unquote
import os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def oneview_segments_generate_onspot_header(ingress: dict):
    """
    Generate a header for OnSpot data and store it in Azure Blob.

    This function creates a CSV header for OnSpot data and uploads it to
    an Azure Blob. The function then returns a URL to the stored header
    in the Azure Blob.

    Parameters
    ----------
    ingress : dict
        Dictionary containing details about the Azure Blob storage.

    Returns
    -------
    dict
        A dictionary containing the URL to the stored header in Azure Blob.

    Notes
    -----
    This function uses the azure.storage.filedatalake library to interact
    with Azure Blob.
    """

    # Initialize Azure Blob client using connection string from environment variables
    blob = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["output"]["conn_str"]],
        container_name=ingress["output"]["container_name"],
        blob_name="{}/raw/{}".format(ingress["output"]["prefix"], "header.csv"),
    )

    # Upload the header data to Azure Blob
    blob.upload_blob(b"street,city,state,zip,zip4", overwrite=True)

    # Generate a SAS token for the stored header in Azure Blob and return the URL
    return {
        "url": (
            unquote(blob.url)
            + "?"
            + generate_blob_sas(
                account_name=blob.account_name,
                container_name=blob.container_name,
                blob_name=blob.blob_name,
                account_key=blob.credential.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + relativedelta(days=2),
            )
        ),
        "columns": None,
    }

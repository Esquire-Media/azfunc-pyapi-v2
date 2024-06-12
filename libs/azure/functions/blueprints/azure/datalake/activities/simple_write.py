# File: libs/azure/functions/blueprints/datalake/activities/simple_write.py

from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from azure.durable_functions import Blueprint
from urllib.parse import unquote
import os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def datalake_simple_write(ingress: dict) -> dict:
    """
    Write content to Azure Blob Storage and return a SAS token URL.

    This function writes the provided content to an Azure Blob Storage container and
    then generates a SAS token URL for accessing the uploaded content.

    Parameters
    ----------
    ingress : dict
        A dictionary containing necessary parameters, specified as follows:
        - conn_str (str): Connection string or the name of the environment variable storing the Azure Blob Storage connection string.
        - container_name (str): Name of the Azure Blob Storage container.
        - blob_name (str): Name of the blob where the content will be written.
        - content (str): Content to be written to the blob.

    Returns
    -------
    dict
        A dictionary containing:
        - url (str): The SAS token URL for accessing the uploaded content in Azure Blob Storage.
        - columns : None, as this function deals with simple content writing.

    Examples
    --------
    In an orchestrator function:

    .. code-block:: python

        import azure.durable_functions as df

        def orchestrator_function(context: df.DurableOrchestrationContext):
            write_info = yield context.call_activity('datalake_simple_write', {
                "conn_str": "AZURE_BLOB_CONNECTION_STRING",
                "container_name": "my-container",
                "blob_name": "sample_blob.txt",
                "content": "This is the content to be written."
            })
            return write_info

    Notes
    -----
    - The function writes the provided content to the specified Azure Blob Storage location.
    - A SAS token URL is generated for the uploaded content, which provides read access for two days.
    """

    # Initialize Azure Blob client using connection string from environment variables
    blob = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["conn_str"]],
        container_name=ingress["container_name"],
        blob_name=ingress["blob_name"],
    )

    # Upload the header data to Azure Blob
    blob.upload_blob(ingress["content"], overwrite=True)

    # Generate a SAS token for the stored header in Azure Blob and return the URL
    return (
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
    )

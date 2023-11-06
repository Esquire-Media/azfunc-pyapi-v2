# File: libs/azure/functions/blueprints/datalake/activities/concat.py

from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from urllib.parse import unquote
import os

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def datalake_concat_blobs(ingress: dict) -> str:
    """
    Concatenate multiple Azure blobs into a single blob.

    Given a list of source blob URLs, this function fetches each blob and appends its content to a new 
    or existing target blob in Azure Blob storage. The blobs are processed in chunks, allowing for efficient 
    concatenation, especially for large blobs.

    Parameters
    ----------
    ingress : dict
        Configuration details for concatenation:
        - conn_str (str): Name of the environment variable that contains the Azure connection string.
        - container_name (str): Name of the Azure Blob storage container where the combined blob will be stored.
        - blob_name (str): Name of the combined blob.
        - copy_source_urls (list[str]): List of source blob URLs to be concatenated.

    Returns
    -------
    str
        SAS URL of the combined blob in Azure Blob storage.

    Examples
    --------
    To concatenate blobs using an orchestrator function:

    .. code-block:: python

        import azure.durable_functions as df

        def orchestrator_function(context: df.DurableOrchestrationContext):
            combined_blob_url = yield context.call_activity('datalake_concat_blobs', {
                "conn_str": "YOUR_AZURE_CONNECTION_STRING_ENV_VARIABLE",
                "container_name": "your-azure-blob-container",
                "blob_name": "combined-blob-name",
                "copy_source_urls": [
                    "https://storage_account_name.blob.core.windows.net/container_name/path/to/blob_01?sastoken_with_read_permission",
                    "https://storage_account_name.blob.core.windows.net/container_name/path/to/blob_02?sastoken_with_read_permission",
                    # ... additional URLs ...
                    "https://storage_account_name.blob.core.windows.net/container_name/path/to/blob_99?sastoken_with_read_permission",
                ]
            })
            return combined_blob_url

    Notes
    -----
    - The function uses the append blob type in Azure Blob storage, which is optimized for append operations.
    - Data from the source blobs is fetched and written in chunks, allowing efficient processing of large blobs.
    - The resulting combined blob's SAS URL provides read access for two days from the time of its generation.
    """

    # Extract connection string from environment variables using the provided key
    connection_string = os.environ[ingress["conn_str"]]
    
    # Initialize Azure Blob client for the output blob using the extracted connection string
    output_blob = BlobClient.from_connection_string(
        conn_str=connection_string,
        container_name=ingress["container_name"],
        blob_name=ingress["blob_name"],
    )

    # Create a new append blob in the specified container
    # This blob will be used to store the combined data from multiple source blobs
    output_blob.create_append_blob()

    # Define the size of each chunk (in bytes) that will be appended
    # Chunks allow for efficient appending, especially for large blobs
    chunk_size = 4 * 1024 * 1024  # 4MB

    # Loop through each source blob URL provided in the ingress dictionary
    for copy_source_url in ingress["copy_source_urls"]:
        # Initialize a BlobClient for the source blob using its URL
        input_blob = BlobClient.from_blob_url(copy_source_url)
        
        # Fetch the size of the source blob to determine how many chunks are needed
        input_size = input_blob.get_blob_properties().size
        
        # Split the source blob into chunks and append each chunk to the output blob
        for i in range(0, input_size, chunk_size):
            output_blob.append_block_from_url(
                copy_source_url=copy_source_url,
                source_offset=i,
                # Determine the size of the current chunk. 
                # If it's the last chunk, it might be smaller than the defined chunk size
                source_length=chunk_size
                if (i + chunk_size) < input_size
                else (input_size - i),
            )

    # Once all source blobs have been appended, return the name of the combined blob
    return (
        unquote(output_blob.url)
        + "?"
        + generate_blob_sas(
            account_name=output_blob.account_name,
            container_name=output_blob.container_name,
            blob_name=output_blob.blob_name,
            account_key=output_blob.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
    )

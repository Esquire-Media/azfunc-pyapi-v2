# File: libs/azure/functions/blueprints/datalake/activities/concat_blobs.py

from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
import os

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def datalake_concat_blobs(ingress: dict) -> str:
    """
    Combine multiple blobs into a single append blob in Azure Data Lake.

    Parameters
    ----------
    ingress : dict
        A dictionary containing necessary parameters, specified as follows:
        - conn_str (str): The name of the environment variable that stores the connection string for Azure Blob Storage.
        - container_name (str): The name of the Azure Blob Storage container where the blobs reside.
        - blob_name (str): The name designated for the output blob, which will contain the concatenated data.
        - copy_source_urls (list of str): A list of URLs, each pointing to a blob to be included in the concatenation process.

    Returns
    -------
    str
        The name of the resulting combined blob.

    Examples
    --------
    Here's how you can call 'datalake_concat_blobs' from an Azure Durable Functions orchestrator function:

    .. code-block:: python

        import azure.durable_functions as df

        def orchestrator_function(context: df.DurableOrchestrationContext):
            result = yield context.call_activity('datalake_concat_blobs', {
                "conn_str": "AZURE_CONNECTION_STRING",
                "container_name": "mycontainer",
                "blob_name": "/path/to/combined_blob",
                "copy_source_urls": [
                    "https://storage_account_name.blob.core.windows.net/container_name/path/to/blob_01?sastoken_with_read_permission",
                    "https://storage_account_name.blob.core.windows.net/container_name/path/to/blob_02?sastoken_with_read_permission",
                    # ... additional URLs ...
                    "https://storage_account_name.blob.core.windows.net/container_name/path/to/blob_99?sastoken_with_read_permission",
                ]
            })
            return result

    Notes
    -----
    - The function employs the `append_block_from_url` method of Azure Blob Storage to perform the concatenation, making it suitable for both text and binary data.
    - To optimize the data transfer process, the function divides each blob into 4MB chunks during the append operation.
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
    return output_blob.blob_name

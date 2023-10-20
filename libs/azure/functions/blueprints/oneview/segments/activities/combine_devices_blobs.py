# File: libs/azure/functions/blueprints/oneview/segments/activities/combine_devices_blobs.py

from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
import os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def oneview_segments_combine_devices_blobs(ingress: dict):
    """
    Combine multiple device blobs into a single blob in Azure Blob Storage.

    This function takes a list of blob URLs and appends the content of each blob
    to a new blob in Azure Blob Storage. The name of the output blob is constructed
    based on the provided SegmentID.

    Parameters
    ----------
    ingress : dict
        Dictionary containing details about the Azure Blob Storage and a list of blob URLs to combine.
        Expected keys are 'output' (which contains connection string details) and 'blobs' (list of blob URLs).

    Returns
    -------
    str
        The name of the combined blob in Azure Blob Storage.

    Notes
    -----
    This function uses the azure.storage.blob library to interact with Azure Blob Storage.
    """

    # Initialize Azure Blob client for the output blob using connection string from environment variables
    output_blob = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["output"]["conn_str"]],
        container_name=ingress["output"]["container_name"],
        blob_name="{}/{}.csv".format(
            ingress["output"]["prefix"], ingress["record"]["SegmentID"]
        ),
    )

    # Create an append blob to combine the data from multiple blobs
    output_blob.create_append_blob()

    # Iterate over the list of blob URLs and append each blob's data to the output blob
    chunk_size = 4 * 1024 * 1024
    for copy_source_url in ingress["blobs"]:
        input_blob = BlobClient.from_blob_url(copy_source_url)
        input_size = input_blob.get_blob_properties().size
        for i in range(0, input_size, chunk_size):
            output_blob.append_block_from_url(
                copy_source_url=copy_source_url,
                source_offset=i,
                source_length=chunk_size
                if (i + chunk_size) < input_size
                else (input_size - i),
            )

    # Return the name of the combined blob
    return output_blob.blob_name

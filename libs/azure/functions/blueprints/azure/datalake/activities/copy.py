# File: libs/azure/functions/blueprints/datalake/activities/copy.py

from azure.durable_functions import Blueprint
from azure.storage.blob import BlobBlock
from libs.utils.azure_storage import get_blob_sas, init_blob_client
import httpx, uuid

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def azure_datalake_copy_blob(ingress: dict) -> str:
    """
    Copy blob data from a source to a target in Azure Blob Storage.

    This function copies blob data from the specified source to the specified target in
    Azure Blob Storage. The copying operation is performed by the Azure Storage service
    directly, meaning that the data is not routed through this Python instance.

    Parameters
    ----------
    ingress : dict
        A dictionary containing necessary parameters:
        - source (str or dict): The source can either be a string URL or a dictionary
                                with "conn_str", "container_name", and "blob_name" keys.
        - target (str or dict): The target can either be a string URL or a dictionary
                                with "conn_str", "container_name", and "blob_name" keys.

    Returns
    -------
    str
        A SAS token URL for accessing the copied content in Azure Blob Storage.

    Examples
    --------
    Using string URLs for source and target:

    .. code-block:: python

        import azure.durable_functions as df

        def orchestrator_function(context: df.DurableOrchestrationContext):
            copied_url = yield context.call_activity('azure_datalake_copy_blob', {
                "source": "source_blob_url_with_read_SAS_token",
                "target": "target_blob_url_with_write_SAS_token"
            })
            return copied_url

    Using dictionary values for source and target:

    .. code-block:: python

        import azure.durable_functions as df

        def orchestrator_function(context: df.DurableOrchestrationContext):
            copied_url = yield context.call_activity('azure_datalake_copy_blob', {
                "source": {
                    "conn_str": "SOURCE_AZURE_BLOB_CONNECTION_STRING",
                    "container_name": "source-container",
                    "blob_name": "source_blob.txt"
                },
                "target": {
                    "conn_str": "TARGET_AZURE_BLOB_CONNECTION_STRING",
                    "container_name": "target-container",
                    "blob_name": "target_blob.txt"
                }
            })
            return copied_url

    Notes
    -----
    - The function utilizes the Azure SDK's `BlobClient` for operations on Azure Blob Storage.
    - The copying operation is server-side, so the data is not streamed through this Python instance.
    """

    if isinstance(ingress["source"], str):
        source_url = ingress["source"]
    else:
        blob = init_blob_client(**ingress["source"])
        source_url = get_blob_sas(blob)

    if isinstance(ingress["target"], str):
        blob = init_blob_client(blob_url=ingress["target"])
    else:
        blob = init_blob_client(**ingress["target"])

    try:
        blob.upload_blob_from_url(
            source_url,
            overwrite=True,
        )
    except:
        # If uploading from url isn't supported (Azurite)
        with httpx.stream("GET", source_url) as response:
            # Ensure the response is successful
            response.raise_for_status()
            # Open a stream to Azure Blob and write chunks as they are being received
            block_list = []
            for chunk in response.iter_bytes():
                blk_id = str(uuid.uuid4())
                blob.stage_block(block_id=blk_id, data=chunk)
                block_list.append(BlobBlock(block_id=blk_id))
            blob.commit_block_list(block_list)

    return get_blob_sas(blob)

# File: libs/azure/functions/blueprints/asw/athena/activities/download.py

from azure.durable_functions import Blueprint
from azure.storage.blob import BlobBlock
from libs.utils.azure_storage import get_blob_sas, init_blob_client
import httpx, uuid

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def aws_athena_activity_download(ingress: dict):
    blob = init_blob_client(**ingress)
    try:
        blob.upload_blob_from_url(
            ingress["url"],
            overwrite=True,
        )
    except:
        # If uploading from url isn't supported (Azurite)
        with httpx.stream("GET", ingress["url"]) as response:
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

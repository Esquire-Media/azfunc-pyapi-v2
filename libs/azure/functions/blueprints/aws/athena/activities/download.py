# File: libs/azure/functions/blueprints/asw/athena/activities/download.py

from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
    BlobBlock,
)
from dateutil.relativedelta import relativedelta
from azure.durable_functions import Blueprint
from urllib.parse import unquote
import datetime, httpx, os, uuid

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def aws_athena_activity_download(ingress: dict):
    blob: BlobClient = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["conn_str"]],
        container_name=ingress["container_name"],
        blob_name=ingress["blob_name"],
    )
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

    return (
        unquote(blob.url)
        + "?"
        + generate_blob_sas(
            account_name=blob.account_name,
            container_name=blob.container_name,
            blob_name=blob.blob_name,
            account_key=blob.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.datetime.utcnow() + relativedelta(days=2),
        )
    )

# File: libs/azure/functions/blueprints/datalake/activities/stage_blob_blocks.py

from libs.azure.functions import Blueprint
from libs.utils.azure_storage import get_blob_sas, init_blob_client
import requests, uuid

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_azureDatalake_stageBlobBlocks(ingress: dict) -> str:
    destination_blob = init_blob_client(**ingress["destination"])
    if destination_blob.api_version >= "2019-12-12":
        max_block_size = 4000 * 1024 * 1024
    elif "2016-05-31" <= destination_blob.api_version <= "2019-07-07":
        max_block_size = 100 * 1024 * 1024
    else:
        max_block_size = 4 * 1024 * 1024

    try:
        source_blob = init_blob_client(**ingress["source"])
        source_url = get_blob_sas(source_blob)
        source_size = source_blob.get_blob_properties().size
    except:
        source_url = ingress["source"]["url"]
        source_size = int(requests.head(source_url).headers["Content-Length"])

    return {
        "index": ingress.get("index", 0),
        "block_ids": [
            block_id
            for i in range(0, source_size, max_block_size)
            if (block_id := uuid.uuid4().hex)
            if destination_blob.stage_block_from_url(
                block_id,
                source_url,
                source_offset=i,
                source_length=min(max_block_size, source_size - i),
            )
        ],
    }

from azure.durable_functions import Blueprint
import requests

from libs.utils.azure_storage import get_blob_sas, init_blob_client

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_getContentLength(ingress: str) -> dict:
    try:
        source_blob = init_blob_client(**ingress)
        source_url = get_blob_sas(source_blob)
        source_size = source_blob.get_blob_properties().size
    except:
        source_url = ingress["source"]["url"]
        source_size = int(requests.head(source_url).headers["Content-Length"])
    return {
        "url": source_url,
        "size": source_size,
    }

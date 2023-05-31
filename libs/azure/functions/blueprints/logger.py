from azure.functions import Context
from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
import os
import logging

bp = Blueprint()


@bp.route(route="logger", methods=["POST"])
async def logger(req: HttpRequest, context: Context):
    data = req.get_body()
    BlobClient.from_connection_string(
        conn_str=os.environ["AzureWebJobsStorage"],
        container_name="general",
        blob_name=context.invocation_id,
    ).upload_blob(data)
    logging.warn(dict(req.params))
    logging.warn(data)
    return HttpResponse("OK")

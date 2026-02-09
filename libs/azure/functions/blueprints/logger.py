from azure.functions import Context
from azure.durable_functions import Blueprint
from azure.functions import HttpRequest, HttpResponse
from libs.utils.azure_storage import init_blob_client
import os
import logging

bp = Blueprint()


@bp.route(route="logger", methods=["POST"])
async def logger(req: HttpRequest, context: Context):
    data = req.get_body()
    init_blob_client(
        conn_str=os.environ["AzureWebJobsStorage"],
        container_name="general",
        blob_name=context.invocation_id,
    ).upload_blob(data)
    logging.warn(dict(req.params))
    logging.warn(data)
    return HttpResponse("OK")

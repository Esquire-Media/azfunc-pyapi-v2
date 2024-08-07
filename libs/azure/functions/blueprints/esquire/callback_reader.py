from azure.durable_functions import Blueprint
from azure.functions import HttpRequest, HttpResponse
from azure.storage.blob import BlobClient
from io import StringIO
import orjson as json, uuid, os, pandas as pd

bp = Blueprint()

@bp.route(route="esquire/callback_reader", methods=["POST"])
async def starter_callbackReader(req: HttpRequest):
    instance_id = str(uuid.uuid4())
    body = req.get_body()

    # export headers to blob
    if req.headers:
        headers = {k:v for k,v in req.headers.items()}
        blob_client = BlobClient.from_connection_string(
            conn_str=os.environ['AzureWebJobsStorage'],
            container_name='callbacks',
            blob_name=f"{instance_id}/headers.json"
        )
        blob_client.upload_blob(
            json.dumps(headers)
        )

    # export body to blob
    if req.get_body():
        body = req.get_body()
        # determine appropriate file format based on if data can be parsed under that format
        try:
            json.loads(body)
            format = 'json'
        except:
            try:
                pd.read_csv(StringIO(body))
                format = 'csv'
            except:
                format = 'txt'
        # connect and upload blob
        blob_client = BlobClient.from_connection_string(
            conn_str=os.environ['AzureWebJobsStorage'],
            container_name='callbacks',
            blob_name=f"{instance_id}/body.{format}"
        )
        blob_client.upload_blob(
            req.get_body()
        )

    return HttpResponse(
        "Success",
        status_code=200
    )
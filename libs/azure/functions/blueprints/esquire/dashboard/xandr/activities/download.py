# File: libs/azure/functions/blueprints/esquire/dashboard/xandr/activities/download.py

from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
from libs.openapi.clients.xandr import XandrAPI
import os
import pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def esquire_dashboard_xandr_activity_download(ingress: dict):
    XA = XandrAPI(asynchronus=False)
    _, report, _ = XA.createRequest("DownloadReport").request(
        parameters={"id": ingress["instance_id"]}
    )
    if ingress.get("conn_str", False):
        conn_str = ingress["conn_str"] or "AzureWebJobsStorage"
    else:
        conn_str = "AzureWebJobsStorage"
    
    blob: BlobClient = BlobClient.from_connection_string(
        conn_str=os.environ.get(conn_str),
        container_name=ingress["container"],
        blob_name=ingress["outputPath"],
    )
    blob.upload_blob(pd.DataFrame(report).to_parquet(), overwrite=True)

    return ""

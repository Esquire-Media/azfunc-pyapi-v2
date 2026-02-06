# File: libs/azure/functions/blueprints/esquire/dashboard/xandr/activities/download.py

from azure.durable_functions import Blueprint
from libs.openapi.clients.xandr import XandrAPI
from libs.utils.azure_storage import init_blob_client
import os, pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def esquire_dashboard_xandr_activity_download(ingress: dict):
    if ingress.get("container"):
        ingress["container_name"] = ingress.get("container")
        del ingress["container"]
        
    XA = XandrAPI(asynchronus=False)
    _, report, _ = XA.createRequest("DownloadReport").request(
        parameters={"id": ingress["instance_id"]}
    )
    if ingress.get("conn_str", False):
        conn_str = ingress["conn_str"] or "AzureWebJobsStorage"
    else:
        conn_str = "AzureWebJobsStorage"
    
    blob = init_blob_client(
        conn_str=os.environ.get(conn_str),
        container_name=ingress["container_name"],
        blob_name=ingress["outputPath"],
    )
    blob.upload_blob(pd.DataFrame(report).to_parquet(), overwrite=True)

    return ""

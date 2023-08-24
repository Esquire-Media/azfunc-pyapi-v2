# File: libs/azure/functions/blueprints/esquire/dashboard/meta/activities/download.py

from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
from libs.openapi.clients.facebook import FacebookAPI
import os, pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def esquire_dashboard_meta_activity_download(ingress: dict):
    FBA = FacebookAPI(asynchronus=False)
    _, report, _ = FBA.createRequest("Download").request(
        parameters={
            "name": "report",
            "format": "csv",
            "report_run_id": ingress["report_run_id"],
        }
    )

    if "No data available." not in report.keys():
        blob: BlobClient = BlobClient.from_connection_string(
            conn_str=os.environ[ingress["conn_str"]],
            container_name=ingress["container"],
            blob_name=ingress["outputPath"],
        )
        df = pd.DataFrame(report)
        blob.upload_blob(df.to_parquet(index=False), overwrite=True)
        # return list(set(df["Ad ID"].to_list()))

    return ""

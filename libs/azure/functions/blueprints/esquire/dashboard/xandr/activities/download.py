# File: libs/azure/functions/blueprints/esquire/dashboard/xandr/activities/download.py

from azure.storage.filedatalake import DataLakeFileClient
from libs.azure.functions import Blueprint
from libs.openapi.clients.xandr import XandrAPI
import os, httpx, json

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def esquire_dashboard_xandr_activity_download(ingress: dict):
    token = XandrAPI.get_token()
    XA = XandrAPI(token, asynchronus=False)
    _, data, _ = XA.createRequest(("/report", "get")).request(
        parameters={"id": ingress["instance_id"]}
    )
    file: DataLakeFileClient = DataLakeFileClient.from_connection_string(
        conn_str=os.environ.get(ingress["conn_str"], os.environ["AzureWebJobsStorage"]),
        file_system=ingress["container"],
        file_path=ingress["outputPath"]
        + json.loads(data.response.report.json_request)["report"]["report_type"],
    )
    with httpx.Client() as client:
        with client.stream(
            method="GET",
            url=str(XA.url) + "/report_download",
            params={"id": ingress["instance_id"]},
            headers={"Authorization": token},
        ) as response:
            offset = 0
            for chunk in response.iter_bytes():
                file.append_data(data=chunk, offset=offset, length=len(chunk))
                offset += len(chunk)
            file.flush_data(offset)

    return ""

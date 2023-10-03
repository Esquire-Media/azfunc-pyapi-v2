# File: libs/azure/functions/blueprints/meta/activities/request.py

from libs.azure.functions import Blueprint
from libs.openapi.clients import Meta
import json, os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def meta_activity_request(ingress: dict):
    factory = Meta[ingress["operationId"]]
    factory.security.setdefault(
        "access_token",
        os.environ.get(
            ingress.get("access_token", ""), 
            os.environ.get("META_ACCESS_TOKEN", "")
        ),
    )
    headers, response, _ = factory.request(
        data=ingress.get("data", None),
        parameters=ingress.get("parameters", None),
    )

    if ingress.get("destination", False) and getattr(response, "data", False):
        from azure.storage.blob import BlobClient
        import pandas as pd, uuid

        blob = BlobClient.from_connection_string(
            conn_str=os.environ[
                ingress["destination"].get("conn_str", "AzureWebJobsStorage")
            ],
            container_name=ingress["destination"]["container"],
            blob_name="{}/{}.parquet".format(
                ingress["destination"]["path"], uuid.uuid4().hex
            ),
        )
        blob.upload_blob(
            pd.DataFrame(response.model_dump()["data"]).to_parquet(index=False)
        )

    return {
        "headers": headers,
        "data": json.loads(response.model_dump_json()),
    }

# File: libs/azure/functions/blueprints/esquire/dashboard/xandr/activities/creatives.py

from libs.azure.functions import Blueprint
from libs.data import from_bind
from libs.openapi.clients.xandr import XandrAPI
import json, os, pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def esquire_dashboard_xandr_activity_creatives(ingress: dict):
    if ingress:
        XA = XandrAPI(asynchronus=False)
        _, response, _ = XA.createRequest("GetCreative").request(
            parameters={
                "num_elements": ingress["num_elements"],
                "start_element": ingress["start_element"],
            }
        )
        blob = None
        if ingress.get("destination", False):
            from azure.storage.blob import BlobClient
            import uuid

            # Create a BlobClient to upload the data to Azure Blob Storage
            blob = BlobClient.from_connection_string(
                conn_str=os.environ[
                    ingress["destination"].get("conn_str", "AzureWebJobsStorage")
                ],
                container_name=ingress["destination"]["container_name"],
                blob_name="{}/{}.parquet".format(
                    ingress["destination"]["blob_prefix"], uuid.uuid4().hex
                ),
            )
            # Upload the data as a parquet file
            blob.upload_blob(
                pd.DataFrame(response.response.creatives).to_parquet(index=False)
            )

        return {
            "status": response.response.status,
            "num_elements": response.response.num_elements,
            "count": response.response.count,
            "data": blob.url
            if blob
            else json.loads(response.model_dump_json()["response"]["creatives"]),
        }
    else:
        provider = from_bind("xandr_dashboard")
        return next(
            iter(
                next(
                    iter(
                        pd.read_sql(
                            sql="SELECT MAX([last_modified]) FROM [creatives]",
                            con=provider.connect().connection(),
                        ).iloc
                    ),
                    ["1970-01-01 00:00:00"],
                )
            )
        )

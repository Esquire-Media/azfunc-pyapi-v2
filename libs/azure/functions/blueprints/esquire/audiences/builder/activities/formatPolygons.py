# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/formatPolygons.py

from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from urllib.parse import unquote
import os, pandas as pd

try:
    import orjson as json
except:
    import json


bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_formatPolygons(ingress: dict):
    input_blob = BlobClient.from_blob_url(ingress["source"])
    df = pd.read_csv(ingress["source"])
    output_blob = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["destination"]["conn_str"]],
        container_name=input_blob.container_name,
        blob_name=input_blob.blob_name,
    )
    output_blob.upload_blob(
        json.dumps(
            [
                feature
                for item in df["polygon"].apply(eval)
                for feature in item.get("features", [])
            ]
        ),
        overwrite=True
    )

    return (
        unquote(output_blob.url)
        + "?"
        + generate_blob_sas(
            account_name=output_blob.account_name,
            container_name=output_blob.container_name,
            blob_name=output_blob.blob_name,
            account_key=output_blob.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
    )

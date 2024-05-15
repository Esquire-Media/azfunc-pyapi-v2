# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/formatOnspotRequest.py

from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
)
from azure.storage.blob._quick_query_helper import BlobQueryReader
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    extract_dates,
)
from urllib.parse import unquote
import os, uuid, pandas as pd, logging
try:
    import orjson as json
except:
    import json


bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_formatOnspotRequest(ingress: dict):
    now = datetime.utcnow()
    if ingress["custom_coding"].get("request", {}):
        start, end = extract_dates(ingress["custom_coding"]["request"], now)
    else:
        start = now - relativedelta(days=33)
        end = now - relativedelta(days=2)
        
    input_blob = BlobClient.from_blob_url(ingress["source_url"])
    features = json.loads(input_blob.download_blob().readall())
    
    request = {
        "type": "FeatureCollection",
        "features": [
            {
                **feature,
                "properties": {
                    "name": uuid.uuid4().hex,
                    "fileName": uuid.uuid4().hex + ".csv",
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "hash": False,
                    "fileFormat": {
                        "delimiter": ",",
                        "quoteEncapsulate": True,
                    },
                },
            }
            for feature in features
        ],
    }
    
    output_blob = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["working"]["conn_str"]],
        container_name=ingress["working"]["container_name"],
        blob_name="{}/{}.json".format(
            ingress["working"]["blob_prefix"], uuid.uuid4().hex
        ),
    )
    output_blob.upload_blob(json.dumps(request))

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

# File: libs/azure/functions/blueprints/asw/athena/activities/download.py

from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from urllib.parse import unquote
import os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def aws_athena_activity_download(ingress: dict):
    blob: BlobClient = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["conn_str"]],
        container_name=ingress["container_name"],
        blob_name=ingress["blob_name"],
    )
    
    blob.upload_blob_from_url(
        ingress["url"],
        overwrite=True,
    )

    return (
        unquote(blob.url)
        + "?"
        + generate_blob_sas(
            account_name=blob.account_name,
            container_name=blob.container_name,
            blob_name=blob.blob_name,
            account_key=blob.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
    )

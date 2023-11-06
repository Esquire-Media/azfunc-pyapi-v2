from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from urllib.parse import unquote
from datetime import datetime as dt, timedelta

def get_blob_sas(blob_client:BlobClient, expiry:timedelta=timedelta(days=2)) -> str:
    """
    Given a BlobClient object, return a SAS URL with permissions enabled and a set expiry time.

    Params
    blob_client     : Azure storage blob client for the selected blob.
    expiry          : [Optional] datetime.timedelta object representing how long the SAS permissions should be enabled for.
    """
    return (
        unquote(blob_client.url)
        + "?"
        + generate_blob_sas(
            account_name = blob_client.account_name,
            container_name = blob_client.container_name,
            blob_name = blob_client.blob_name,
            account_key = blob_client.credential.account_key,
            permission = BlobSasPermissions(read=True),
            expiry = dt.utcnow() + expiry
        )
    )
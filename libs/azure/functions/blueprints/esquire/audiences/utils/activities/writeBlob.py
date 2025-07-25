import pandas as pd
from io import StringIO
from azure.storage.blob import BlobServiceClient
import os
from azure.durable_functions import Blueprint

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_writeBlob(payload: dict) -> str:
    records = payload["records"]
    container = payload["container"]
    blob_name = payload["blobName"]

    df = pd.DataFrame(records)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    blob_service = BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
    blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)

    return blob_client.url
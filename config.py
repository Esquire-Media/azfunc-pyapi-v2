from azure.storage.blob import BlobServiceClient
from libs.data import register_binding
import os

register_binding(
    "AzureWebJobsStorageBlobs",
    "KeyValue",
    "azure_blob",
    transport_params={
        "client": BlobServiceClient.from_connection_string(
            os.environ["AzureWebJobsStorage"]
        )
    },
)
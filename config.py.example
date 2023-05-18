# Data Bindings
from azure.storage.blob import BlobServiceClient
from azure.data.tables import TableServiceClient
import libs.data
import os

libs.data.register_binding(
    "general_blobs",
    "KeyValue",
    "azure_blob",
    transport_params={
        "client": BlobServiceClient.from_connection_string(
            os.environ["AzureWebJobsStorage"]
        )
    },
)
libs.data.register_binding(
    "some_azure_tables",
    "KeyValue",
    "azure_table",
    client=TableServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"]),
)
libs.data.register_binding(
    "an_sql_server",
    "Structured",
    "sql",
    url=os.environ["DATABIND_SQL_GENERAL"],
    schemas=["esquire", "dbo"],
)

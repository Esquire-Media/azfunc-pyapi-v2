# File: libs/azure/functions/blueprints/postgres/activities/getRecordCount.py

from azure.durable_functions import Blueprint
from libs.data import from_bind
from libs.utils.azure_storage import get_blob_sas, init_blob_client
import os, pandas as pd, uuid

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_azurePostgres_resultToBlob(ingress: dict) -> int:
    # ingress = {
    #     "source": {
    #         "bind": "BIND_HANDLE",
    #         "query": "SELECT * FROM table",
    #     },
    #     "destination": {
    #         "conn_str": "YOUR_AZURE_CONNECTION_STRING_ENV_VARIABLE",
    #         "container_name": "your-azure-blob-container",
    #         "blob_prefix": "combined-blob-name",
    #         "format": "CSV",
    #     },
    #     "limit": 100,
    #     "offset": 0
    # }

    # Establish a session with the database using the provided bind information.
    df = pd.read_sql_query(
        sql="{} LIMIT {} OFFSET {}".format(
            ingress["source"]["query"], ingress["limit"], ingress["offset"]
        ),
        con=from_bind(ingress["source"]["bind"]).connect().connection(),
    )

    # Initialize Azure Blob client for the output blob using the extracted connection string
    output_blob = init_blob_client(
        conn_str=os.environ[ingress["destination"]["conn_str"]],
        container_name=ingress["destination"]["container_name"],
        blob_name="{}/{}.{}".format(
            ingress["destination"]["blob_prefix"],
            uuid.uuid4().hex,
            ingress["destination"]["format"].lower(),
        ),
    )

    match ingress["destination"]["format"]:
        case "CSV":
            output_blob.upload_blob(df.to_csv())
        case _:
            raise Exception(
                "Format not supported: {}".format(ingress["destination"]["format"])
            )

    return get_blob_sas(output_blob)

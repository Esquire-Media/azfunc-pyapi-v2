from azure.durable_functions import Blueprint
from libs.data import from_bind
from libs.utils.azure_storage import get_blob_sas, init_blob_client
import os
import pandas as pd

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_azurePostgres_resultToBlob(ingress: dict) -> str:
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
    #         "blob_name": "blob/prefix/offset-0.csv"  # provided by orchestrator
    #     },
    #     "limit": 100,
    #     "offset": 0
    # }

    src = ingress["source"]
    dest = ingress["destination"]

    limit = int(ingress["limit"])
    offset = int(ingress["offset"])
    ext = dest["format"].lower()

    # Prefer deterministic blob name from orchestrator; fall back to offset-based
    blob_name = dest.get("blob_name") or f'{dest["blob_prefix"]}/offset-{offset}.{ext}'

    # Execute chunked query
    df = pd.read_sql_query(
        sql=f'{src["query"]} LIMIT {limit} OFFSET {offset}',
        con=from_bind(src["bind"]).connect().connection(), # type: ignore
    )

    # Initialize blob client and upload idempotently
    output_blob = init_blob_client(
        conn_str=os.environ[dest["conn_str"]],
        container_name=dest["container_name"],
        blob_name=blob_name,
    )

    # overwrite=True ensures idempotence on activity retries
    output_blob.upload_blob(df.to_csv(index=False), overwrite=True)

    return get_blob_sas(output_blob)

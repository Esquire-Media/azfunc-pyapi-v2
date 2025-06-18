from azure.durable_functions import Blueprint
import pandas as pd, io, os
from io import BytesIO
from azure.storage.blob.aio import ContainerClient

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
async def activity_readBlob(settings: dict):
    conn_str = os.environ['SALES_INGEST_CONN_STR']
    container_client = ContainerClient.from_connection_string(conn_str, container_name="ingest")

    dfs = []
    async with container_client:
        blob_list = []
        async for blob in container_client.list_blobs(name_starts_with=f"{settings['metadata']['upload_id']}/"):
            blob_list.append(blob)

        blob_list.sort(key=lambda b: b.name)

        for blob in blob_list:
            blob_client = container_client.get_blob_client(blob)
            stream = await blob_client.download_blob()
            content = await stream.readall()
            df = pd.read_csv(
                BytesIO(content),
                dtype="str",
                delimiter=","
                )
            dfs.append(df)

    settings['sales'] = pd.concat(dfs, ignore_index=True)
    return settings

#  file path:libs/azure/functions/blueprints/esquire/audiences/utils/activities/getMaids.py
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    BlobSasPermissions,
    ContainerClient,
    generate_blob_sas,
)
from sqlalchemy import create_engine, text
from libs.azure.functions import Blueprint
import os, logging, json, uuid, pandas as pd

bp: Blueprint = Blueprint()


# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
async def activity_esquireAudiencesUtils_getMaids(ingress: dict):
    # ingress = {
    #     "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
    #     "container_name": "general",
    #     "path_to_blobs": blob_path,
    # }
    engine = create_engine(os.environ["DATABIND_SQL_AUDIENCES"])

    logging.warning(ingress['path_to_blobs'])
    return {}
    # SQL query to count unique device IDs
    count_query = text(
        f"""
        SELECT COUNT(DISTINCT deviceid) FROM OPENROWSET(
        BULK '{ingress["container_name"]}/{ingress['path_to_blobs']}*',
        DATA_SOURCE = 'sa_esqdevdurablefunctions',
        FORMAT = 'CSV'
        ) 
        WITH (
            deviceid VARCHAR(36)
        )
        AS [data]
    """
    )

    # SQL query to get the first 10000 unique device IDs
    fetch_query = text(
        f"""
        SELECT DISTINCT deviceid FROM OPENROWSET(
        BULK '{ingress["container_name"]}/{ingress['path_to_blobs']}*',
        DATA_SOURCE = 'sa_esqdevdurablefunctions',
        FORMAT = 'CSV'
        ) 
        WITH (
            deviceid VARCHAR(36)
        )
        AS [data]
        ORDER BY deviceid
        OFFSET 0 ROWS
        FETCH NEXT 10000 ROWS ONLY
    """
    )

    full_maids = []
    # Execute the queries and fetch the results
    with engine.connect() as connection:
        # Count unique device IDs
        result = connection.execute(count_query)
        unique_device_count = result.scalar()
        print(f"Total count of unique deviceids: {unique_device_count}")

        # Fetch the first 10,000 unique device IDs
        result = connection.execute(fetch_query)
        unique_devices = result.fetchall()
        unique_devices_list = [row[0] for row in unique_devices]
        full_maids.append(unique_devices_list)

    # save each list of 10,000 to a blob
    chunks = split_list(unique_devices_list, 10000)
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(ingress["conn_str"])

    # # Create the container if it doesn't exist
    # container_client = blob_service_client.get_container_client(ingress{"container_name"})
    # urls = []
    # for chunk in chunks:
    #     df = pd.DataFrame(chunk)
    #     # Define the CSV file path
    #     csv_file_path = f'{str(uuid.uuid4())}.csv'

    #     # Save the DataFrame to a CSV file
    #     df.to_csv(csv_file_path, header=["deviceids"], index=False)
        
    #     # Upload the created file
    #     with open(csv_file_path, "rb") as data:        
    #         blob_client = blob_service_client.get_blob_client(container=ingress{"container_name"}, blob=f'{folder_path}10k_lists/{csv_file_path}')
    #         blob_client.upload_blob(data, overwrite=True)
    #         urls.append(blob_client.url)
        

    # # Clean up the local file
    # os.remove(csv_file_path)
    return (full_maids, unique_device_count)


def split_list(original_list, chunk_size):
    # List to hold the resulting chunks
    chunks = []
    # Iterate over the original list in steps of chunk_size
    for i in range(0, len(original_list), chunk_size):
        # Append the current chunk to the list of chunks
        chunks.append(original_list[i : i + chunk_size])
    return chunks

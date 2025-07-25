
from azure.durable_functions import Blueprint
import os
from libs.utils.esquire.neighbors.logic_async import load_estated_data_partitioned_blob, find_neighbors_for_street
from io import StringIO
import csv
from azure.storage.blob import BlobServiceClient
import pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
async def activity_esquireAudiencesBuilder_findNeighbors(ingress: dict):
    city = ingress["city"].strip().lower().replace(" ", "_")
    state = ingress["state"].strip().upper()
    zip_code = ingress["zip"].strip()
    input_blob_url = ingress["inputBlob"]
    N = ingress.get("N", 100)
    same_side_only = ingress.get("same_side_only", True)

    # Parse blob URL
    blob_parts = input_blob_url.split("/")
    container = blob_parts[3]
    blob_name = "/".join(blob_parts[4:])

    # Set up blob client
    blob_service = BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
    blob_data = blob_client.download_blob().readall().decode("utf-8")

    # Filter addresses in this partition
    addresses = []
    reader = csv.DictReader(StringIO(blob_data))
    for row in reader:
        if (
            row.get("city", "").strip().lower() == city and
            row.get("state", "").strip().upper() == state and
            row.get("zipCode", "").strip() == zip_code
        ):
            addresses.append(row)

    if not addresses:
        return []

    # Load estated data for this partition
    try:
        estated_data = await load_estated_data_partitioned_blob(
            f"estated_partition_testing/state={state}/zip_code={zip_code}/city={city}/"
        )
    except Exception as e:
        return None
    
    if estated_data.empty:
        return []

    # Run neighbor matching
    group_results = []
    for street_name, street_addresses in pd.DataFrame(addresses).groupby("street_name"):
        street_data = estated_data[
            estated_data["street_name"] == str(street_name).upper()
        ]
        if street_data.empty:
            continue

        neighbors = find_neighbors_for_street(
            street_data, street_addresses, N, same_side_only
        )
        if not neighbors.empty:
            group_results.append(neighbors)

    if group_results:
        return pd.concat(group_results, ignore_index=True)
    else:
        return None

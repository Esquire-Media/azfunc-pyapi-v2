
from azure.durable_functions import Blueprint
import os
from libs.utils.esquire.neighbors.logic_async import load_estated_data_partitioned_blob, find_neighbors_for_street
from io import StringIO
from azure.storage.blob import BlobClient
import csv
from azure.storage.blob import BlobServiceClient
import pandas as pd
# import logging

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
async def activity_esquireAudiencesNeighbors_findNeighbors(ingress: dict):

    city = ingress["city"].strip().upper().replace(" ", "_")
    state = ingress["state"].strip().upper()
    zip_code = ingress["zip"].strip()
    source_urls = ingress.get("source_urls", [])
    n_per_side = ingress.get("n_per_side")
    same_side_only = ingress.get("same_side_only")

    # logging.warning(f"[LOG] Finding neighbors for {city}, {state}, {zip_code} across {len(source_urls)} url(s)")

    # Aggregate & filter addresses for this partition from ALL sources
    addresses = []
    for url in source_urls:
        try:
            blob_client = BlobClient.from_blob_url(url)
            csv_bytes = blob_client.download_blob().readall()
        except Exception as e:
            # logging.warning(f"[LOG] Failed to read {url}: {e}")
            continue
        reader = csv.DictReader(StringIO(csv_bytes.decode("utf-8")))
        for row in reader:
            if (
                row.get("city", "").strip().upper() == city and
                row.get("state", "").strip().upper() == state and
                row.get("zipCode", "").strip() == zip_code
            ):
                addresses.append(row)
                

    if not addresses:
        # logging.warning(f"[LOG] No addresses for {city}, {state}, {zip_code}")
        return []

    # Load estated data for this partition
    try:
        estated_data = await load_estated_data_partitioned_blob(
            f"estated_partition_testing/state={state}/zip_code={zip_code}/city={city}/"
        )
    except Exception as e:
        # logging.warning("[LOG] Failed to load estated")
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
        street_data = street_data

        neighbors = find_neighbors_for_street(
            street_data, street_addresses.rename(columns={'primary_number':'street_number'}, errors='ignore'), n_per_side, same_side_only
        )
        if not neighbors.empty:
            group_results.append(neighbors)

    if group_results:
        # logging.warning("[LOG] Got group results")
        return pd.concat(group_results, ignore_index=True)[['address', 'city', 'state', 'zipCode', 'plus4Code']].to_dict(orient="records")
    else:
        # logging.warning("[LOG] No group results")
        return None

import requests
import csv
from io import StringIO
from azure.durable_functions import Blueprint
from azure.storage.blob import BlobClient
import logging

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesNeighbors_extractPartitions(ingress: dict) -> list[str]:

    urls = ingress.get("source_urls", [])
    # logging.warning(f"[LOG] Source urls: {urls}")
    if not urls:
        # logging.warning(f"[LOG] No urls found.")
        return []
    
    seen = set()
    out = []

    for url in urls:
        # logging.warning(f"[LOG] url: {url}")
        try:
            blob_client = BlobClient.from_blob_url(url)
            csv_bytes = blob_client.download_blob().readall()
        except Exception as e:
            # logging.warning(f"[LOG] Failed to read {url}: {e}")
            continue
        reader = csv.DictReader(StringIO(csv_bytes.decode("utf-8")))

        for row in reader:
            city = row.get("city_name")
            state = row.get("state_abbreviation")
            zip_code = row.get("zipcode")

            if not(city and state and zip_code):
                # logging.warning(f"[LOG] not all parts found: {city}; {state}; {zip_code}")
                continue

            key = (city.lower().strip(), state.upper().strip(), zip_code.strip())
            if key in seen:
                continue

            out.append({"city": city, "state": state, "zip": zip_code})
            seen.add(key)
            # logging.warning(f"[LOG] Added {key}")


    return out
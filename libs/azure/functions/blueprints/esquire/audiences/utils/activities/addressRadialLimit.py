
import csv
from io import StringIO
from azure.durable_functions import Blueprint
from azure.storage.blob import BlobClient
import logging
from sklearn.neighbors import BallTree
import numpy as np

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesNeighbors_addressRadialLimit(ingress: dict) -> list[str]:

    sales_blob_url = ingress["sales_blob_url"]
    owned_locations = ingress["owned_locations"]
    radius_miles = ingress["radius_miles"]  # rename this param to reflect miles

    sales_records = load_csv_from_blob(sales_blob_url)
    return filter_by_radius(sales_records, owned_locations, radius_miles)



def filter_by_radius(sales_records, owned_locations, radius_miles):
    if not owned_locations or not sales_records:
        return []

    def to_coords(record):
        try:
            return float(record["latitude"]), float(record["longitude"])
        except Exception:
            return None

    sales_coords = [to_coords(r) for r in sales_records]
    owned_coords = [to_coords(o) for o in owned_locations]

    sales_clean = [(r, c) for r, c in zip(sales_records, sales_coords) if c]
    owned_clean = [c for c in owned_coords if c]

    if not sales_clean or not owned_clean:
        return []

    sales_array = np.radians([c for _, c in sales_clean])
    owned_array = np.radians(owned_clean)

    radius_radians = radius_miles / 3958.8  # convert to radians for haversine

    tree = BallTree(owned_array, metric="haversine")
    matches = tree.query_radius(sales_array, r=radius_radians)

    kept_records = [rec for (rec, hits) in zip((r for r, _ in sales_clean), matches) if len(hits) > 0]
    return kept_records

def load_csv_from_blob(blob_url: str) -> list:
    blob = BlobClient.from_blob_url(blob_url)
    raw_bytes = blob.download_blob().readall()
    text = raw_bytes.decode("utf-8")
    
    return list(csv.DictReader(StringIO(text)))

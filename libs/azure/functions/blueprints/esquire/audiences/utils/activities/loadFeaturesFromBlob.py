from azure.durable_functions import Blueprint
from libs.utils.azure_storage import init_blob_client
import json

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_load_features_from_url(ingress: dict) -> dict:
    """
    Input:  {"source_url": "<sas url to json list of features>"}
    Output: {"features": [ ... ]}  (what OnSpot requires)
    """
    url = ingress["source_url"]
    blob = init_blob_client(blob_url=url)
    raw = blob.download_blob().readall()
    doc = json.loads(raw.decode("utf-8"))

    # Accept either:
    # - list of Feature dicts
    # - FeatureCollection dict with "features"
    if isinstance(doc, list):
        return {"features": doc}
    if isinstance(doc, dict) and isinstance(doc.get("features"), list):
        return {"features": doc["features"]}

    raise ValueError("Invalid autopoly blob format; expected list[Feature] or FeatureCollection")

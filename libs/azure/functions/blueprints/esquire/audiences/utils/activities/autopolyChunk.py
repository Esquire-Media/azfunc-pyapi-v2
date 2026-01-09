from azure.durable_functions import Blueprint
from azure.storage.blob import BlobClient
import csv
import io
import logging
import requests
import time
import random

bp = Blueprint()

OVERPASS_URL = "https://overpass-api.de/api/interpreter"


@bp.activity_trigger(input_name="ingress")
def activity_faf_autopoly_from_chunk(ingress: dict):
    chunk_url = ingress["chunk_url"]
    fallback_buffer_m = int(ingress.get("fallback_buffer_m", 20))

    blob = BlobClient.from_blob_url(chunk_url)
    downloader = blob.download_blob()

    # chunk files are already bounded; readall is acceptable here
    # (the big input is handled by the split activity)
    text = downloader.readall().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    features = []
    session = requests.Session()

    for row in reader:
        try:
            lat = float(row["latitude"])
            lon = float(row["longitude"])
        except Exception:
            continue

        poly = _query_osm_building_polygon(session, lat, lon) or _buffer_point(lat, lon, fallback_buffer_m)

        features.append({
            "type": "Feature",
            "geometry": poly,
            "properties": {
                "address_id": row.get("address_id"),
            },
        })

    if not features:
        return None

    return {"type": "FeatureCollection", "features": features}


def _buffer_point(lat: float, lon: float, meters: int):
    delta = meters / 111_320.0
    return {
        "type": "Polygon",
        "coordinates": [[
            [lon - delta, lat - delta],
            [lon + delta, lat - delta],
            [lon + delta, lat + delta],
            [lon - delta, lat + delta],
            [lon - delta, lat - delta],
        ]],
    }


def _query_osm_building_polygon(session, lat, lon, retries=3):
    query = f"""
    [out:json];
    (
      way(around:25,{lat},{lon})["building"];
      relation(around:25,{lat},{lon})["building"];
    );
    out geom;
    """

    for attempt in range(retries):
        try:
            r = session.post(OVERPASS_URL, data=query, timeout=30)
            r.raise_for_status()
            data = r.json()

            for el in data.get("elements", []):
                geom = el.get("geometry")
                if geom:
                    return {
                        "type": "Polygon",
                        "coordinates": [[
                            [p["lon"], p["lat"]] for p in geom
                        ]],
                    }
            return None

        except Exception as e:
            if attempt == retries - 1:
                logging.warning(
                    "OSM failed for lat=%s lon=%s, falling back to buffer: %s",
                    lat,
                    lon,
                    str(e),
                )
                return None

            # polite backoff with jitter
            time.sleep(1.5 * (attempt + 1) + random.random())


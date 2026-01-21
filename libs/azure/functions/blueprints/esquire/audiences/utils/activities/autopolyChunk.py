# File: /libs/azure/functions/blueprints/esquire/audiences/utils/activities/faf_autopoly_chunk_to_polygon_csv.py

from azure.durable_functions import Blueprint
import csv, io, json, logging, time
from typing import Optional

from libs.utils.azure_storage import init_blob_client, get_blob_sas

bp = Blueprint()


def _buffer_point(lat: float, lon: float, meters: int) -> dict:
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


def _osm_polygon(lat: float, lon: float, dist_m: int) -> Optional[dict]:
    try:
        import osmnx as ox
    except Exception:
        return None

    ox.settings.use_cache = True
    gdf = ox.features_from_point((lat, lon), tags={"building": True}, dist=dist_m)
    if gdf is None or gdf.empty:
        return None

    gdf = gdf[gdf.geometry.notnull()].copy()
    if gdf.empty:
        return None

    geom = gdf.iloc[0].geometry
    if geom.geom_type == "Polygon":
        return {"type": "Polygon", "coordinates": [list(geom.exterior.coords)]}
    if geom.geom_type == "MultiPolygon":
        poly = max(list(geom.geoms), key=lambda p: p.area)
        return {"type": "Polygon", "coordinates": [list(poly.exterior.coords)]}
    return None


@bp.activity_trigger(input_name="ingress")
def activity_faf_autopoly_chunk_to_polygon_csv(ingress: dict) -> Optional[str]:
    chunk_url = ingress["chunk_url"]
    working = ingress["working"]
    output_prefix = ingress["output_prefix"]
    fallback_buffer_m = int(ingress.get("fallback_buffer_m", 20))

    osm_cfg = ingress.get("osm", {}) or {}
    osm_enabled = bool(osm_cfg.get("enabled", False))
    osm_dist_m = int(osm_cfg.get("dist_m", 30))
    osm_sleep_s = float(osm_cfg.get("sleep_s", 0.0))

    src = init_blob_client(blob_url=chunk_url)
    text = src.download_blob().readall().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    # output name derived from chunk part-xxxxx.csv -> part-xxxxx.csv under /polygons
    chunk_name = chunk_url.split("?")[0].split("/")[-1]
    dst_blob_name = f"{output_prefix}/{chunk_name}"

    out = io.StringIO(newline="")
    w = csv.DictWriter(out, fieldnames=["polygon"])
    w.writeheader()

    rows = 0
    for row in reader:
        try:
            lat = float(row["latitude"])
            lon = float(row["longitude"])
        except Exception:
            continue

        poly = None
        if osm_enabled:
            try:
                poly = _osm_polygon(lat, lon, osm_dist_m)
            except Exception as e:
                logging.warning("faf_autopoly: osm failed lat=%s lon=%s err=%s", lat, lon, str(e))
                poly = None
            if osm_sleep_s > 0:
                time.sleep(osm_sleep_s)

        if not poly:
            poly = _buffer_point(lat, lon, fallback_buffer_m)

        # one FeatureCollection per row; formatPolygons will flatten features across rows
        fc = {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": poly}]}

        w.writerow({"polygon": json.dumps(fc)})
        rows += 1

    if rows == 0:
        return None

    dst = init_blob_client(
        conn_str=working["conn_str"],
        container_name=working["container_name"],
        blob_name=dst_blob_name,
    )
    dst.upload_blob(out.getvalue().encode("utf-8"), overwrite=True)
    return get_blob_sas(dst)

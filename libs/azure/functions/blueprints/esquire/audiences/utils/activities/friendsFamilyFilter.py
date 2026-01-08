# File: libs/azure/functions/blueprints/esquire/audiences/utils/activities/friendsFamilyFilter.py

from azure.durable_functions import Blueprint
import csv, io, logging
from libs.utils.azure_storage import init_blob_client, get_blob_sas, export_dataframe
from pandas import DataFrame

bp = Blueprint()


class _ChunksIO(io.RawIOBase):
    def __init__(self, chunks_iter):
        self._iter = iter(chunks_iter)
        self._buf = b""
    def readable(self): return True
    def readinto(self, b):
        while len(self._buf) < len(b):
            try: self._buf += next(self._iter)
            except StopIteration: break
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n


@bp.activity_trigger(input_name="ingress")
def activity_faf_filter_countgroupedbydevice_to_deviceids(ingress: dict):
    source_url = ingress["source_url"]
    destination = ingress["destination"]
    output_name = ingress["output_name"]
    thresholds = ingress.get("thresholds", {}) or {}

    min_count = int(thresholds.get("min_count", 2))
    top_n_raw = thresholds.get("top_n", None)
    top_n = int(top_n_raw) if top_n_raw not in (None, False, 0, "0") else None

    dst_blob_name = f"{destination['blob_prefix']}/{output_name}"

    src = init_blob_client(blob_url=source_url)
    downloader = src.download_blob()
    raw = _ChunksIO(downloader.chunks())
    text = io.TextIOWrapper(io.BufferedReader(raw), encoding="utf-8", newline="")
    reader = csv.DictReader(text)

    # maintain descending list capped at N (bounded memory when top_n is set)
    candidates: list[tuple[str, int]] = []

    def maybe_add(deviceid: str, count: int):
        nonlocal candidates
        if top_n is None:
            candidates.append((deviceid, count))
            return
        inserted = False
        for i, (_, c) in enumerate(candidates):
            if count > c:
                candidates.insert(i, (deviceid, count))
                inserted = True
                break
        if not inserted:
            candidates.append((deviceid, count))
        if len(candidates) > top_n:
            candidates.pop()

    for row in reader:
        try:
            deviceid = row.get("deviceid")
            if not deviceid:
                continue
            count = int(row.get("count", 0))
            if count < min_count:
                continue
            maybe_add(deviceid, count)
        except Exception:
            continue

    if not candidates:
        logging.info("faf_filter: empty after filtering")
        return None

    # Write using the same storage helper pattern as write_blob: export_dataframe
    blob_prefix = destination["blob_prefix"].strip("/")
    blob_name = f"{blob_prefix}/{output_name}"

    return export_dataframe(
        df=DataFrame({"deviceid": [d for (d, _) in candidates]}),
        destination={
            "conn_str": destination["conn_str"],
            "container_name": destination["container_name"],
            "blob_name": blob_name,
            "format": "csv",
        },
    )
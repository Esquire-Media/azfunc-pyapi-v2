# File: /libs/azure/functions/blueprints/esquire/audiences/utils/activities/faf_filter_count_blob_to_deviceids.py

from azure.durable_functions import Blueprint
import csv, io, logging
import pandas as pd
from typing import Optional

from libs.utils.azure_storage import init_blob_client, export_dataframe

bp = Blueprint()


class _ChunksIO(io.RawIOBase):
    def __init__(self, chunks_iter):
        self._iter = iter(chunks_iter)
        self._buf = b""

    def readable(self):
        return True

    def readinto(self, b):
        while len(self._buf) < len(b):
            try:
                self._buf += next(self._iter)
            except StopIteration:
                break
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n


@bp.activity_trigger(input_name="ingress")
def activity_faf_filter_count_blob_to_deviceids(ingress: dict) -> Optional[str]:
    source_url = ingress["source_url"]
    thresholds = ingress.get("thresholds", {}) or {}

    min_count = int(thresholds.get("min_count", 2))
    top_n_raw = thresholds.get("top_n", None)
    top_n = int(top_n_raw) if top_n_raw not in (None, False, 0, "0") else None

    src = init_blob_client(blob_url=source_url)
    downloader = src.download_blob()
    raw = _ChunksIO(downloader.chunks())
    text = io.TextIOWrapper(io.BufferedReader(raw), encoding="utf-8", newline="")
    reader = csv.DictReader(text)

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

    rows_read = 0
    for row in reader:
        rows_read += 1
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
        logging.info("faf_filter: empty after filtering, source=%s rows=%s", source_url, rows_read)
        return None

    df = pd.DataFrame({"deviceid": [d for d, _ in candidates]})

    return export_dataframe(
        df=df,
        destination={
            "conn_str": ingress.get("conn_str", "AzureWebJobsStorage"),
            "container_name": ingress["container_name"],
            "blob_prefix": ingress["blob_prefix"].strip("/"),
            "format": "csv",
        },
    )

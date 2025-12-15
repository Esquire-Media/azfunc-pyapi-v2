from azure.durable_functions import Blueprint
from azure.storage.blob import BlobClient
from azure.core.exceptions import ResourceNotFoundError
import csv
import io
import logging

bp = Blueprint()


class _ChunksIO(io.RawIOBase):
    def __init__(self, chunks_iter):
        self._iter = iter(chunks_iter)
        self._buf = b""
        self._closed = False

    def readable(self):
        return True

    def readinto(self, b):
        if self._closed:
            return 0
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
def activity_faf_filter_devices_blob(ingress: dict):
    source_url = ingress["source_url"]
    destination = ingress["destination"]
    output_name = ingress["output_name"]  # deterministic (e.g. {source_key}.csv)
    thresholds = ingress.get("thresholds", {})

    min_visits = int(thresholds.get("min_visits", 1))
    min_distinct_days = int(thresholds.get("min_distinct_days", 1))
    min_dwell = float(thresholds.get("min_dwell_minutes", 0))

    dst_blob_name = f"{destination['blob_prefix']}/{output_name}"

    src = BlobClient.from_blob_url(source_url)
    downloader = src.download_blob()
    raw = _ChunksIO(downloader.chunks())
    text = io.TextIOWrapper(io.BufferedReader(raw), encoding="utf-8", newline="")

    reader = csv.DictReader(text)

    dst = BlobClient.from_connection_string(
        conn_str=destination["conn_str"],
        container_name=destination["container_name"],
        blob_name=dst_blob_name,
    )

    # idempotent overwrite for append blob
    try:
        dst.delete_blob()
    except Exception:
        pass
    dst.create_append_blob()

    # write header once
    dst.append_block(b"deviceid\n")

    written = 0
    batch = io.StringIO(newline="")
    w = csv.writer(batch)

    min_count = int(thresholds.get("min_count", 2))
    max_devices = thresholds.get("max_devices_per_source")  # optional

    kept = 0

    for row in reader:
        try:
            deviceid = row.get("deviceid")
            count = int(row.get("count", 0))

            if not deviceid:
                continue
            if count < min_count:
                continue
        except Exception:
            continue

        w.writerow([deviceid])
        kept += 1

        if max_devices and kept >= max_devices:
            break


        if batch.tell() > 256_000:  # flush ~256KB
            dst.append_block(batch.getvalue().encode("utf-8"))
            batch = io.StringIO(newline="")
            w = csv.writer(batch)

    if batch.tell() > 0:
        dst.append_block(batch.getvalue().encode("utf-8"))

    if written == 0:
        # empty audience shard: keep behavior consistent with other steps (return None)
        try:
            dst.delete_blob()
        except Exception:
            pass
        return None

    return dst.url

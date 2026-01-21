# File: libs/azure/functions/blueprints/esquire/audiences/utils/activities/chunkLatLon.py

from azure.durable_functions import Blueprint
import csv, io, logging
from libs.utils.azure_storage import init_blob_client, get_blob_sas

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
def activity_faf_chunk_sales_latlon_csv(ingress: dict):
    source_url = ingress["source_url"]
    destination = ingress["destination"]
    rows_per_chunk = int(ingress.get("rows_per_chunk", 100))
    required_fields = set(ingress.get("required_fields", []))

    src = init_blob_client(blob_url=source_url)
    downloader = src.download_blob()
    raw = _ChunksIO(downloader.chunks())
    text = io.TextIOWrapper(io.BufferedReader(raw), encoding="utf-8-sig", newline="")

    reader = csv.DictReader(text)
    headers = [h.strip() for h in (reader.fieldnames or [])]

    if required_fields and not required_fields.issubset(set(headers)):
        logging.warning("faf_chunk: missing required fields: %s", required_fields - set(headers))
        return []

    chunk_urls = []
    buf = []
    chunk_index = 0

    def flush():
        nonlocal chunk_index, buf
        if not buf:
            return

        blob_name = f"{destination['blob_prefix']}/part-{chunk_index:05d}.csv"
        chunk_index += 1

        out = io.StringIO(newline="")
        w = csv.DictWriter(out, fieldnames=headers)
        w.writeheader()
        w.writerows(buf)

        dst = init_blob_client(
            conn_str=destination["conn_str"],
            container_name=destination["container_name"],
            blob_name=blob_name,
        )
        dst.upload_blob(out.getvalue().encode("utf-8"), overwrite=True)
        chunk_urls.append(get_blob_sas(dst))
        buf = []

    for row in reader:
        buf.append(row)
        if len(buf) >= rows_per_chunk:
            flush()

    flush()
    return chunk_urls

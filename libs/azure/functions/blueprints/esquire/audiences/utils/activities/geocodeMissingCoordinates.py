"""Optional CSV coordinate enrichment for spatial audience steps.

This activity does not call AddrAll and never changes canonical address columns/IDs.
"""
from __future__ import annotations

import csv
import io
import logging
import os
import tempfile
from collections import Counter
from typing import Any

from azure.durable_functions import Blueprint

from libs.utils.address_coordinates import COORD_COLUMNS, enrich_coordinate_rows
from libs.utils.azure_storage import get_blob_sas, init_blob_client

bp = Blueprint()


class _ChunksIO(io.RawIOBase):
    """Adapt Azure blob downloader.chunks() to a streaming CSV reader."""

    def __init__(self, chunks):
        self._chunks = iter(chunks)
        self._buffer = b""

    def readable(self):
        return True

    def readinto(self, destination):
        while len(self._buffer) < len(destination):
            try:
                self._buffer += next(self._chunks)
            except StopIteration:
                break
        count = min(len(destination), len(self._buffer))
        destination[:count] = self._buffer[:count]
        self._buffer = self._buffer[count:]
        return count


def _write_csv_rows(handle, names: list[str], rows: list[dict[str, Any]], *, header: bool) -> None:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=names)
    if header:
        writer.writeheader()
    writer.writerows(rows)
    handle.write(buffer.getvalue().encode("utf-8"))


def enrich_coordinate_csv(
    source_url: str,
    destination: dict[str, Any],
    *,
    output_name: str = "geocoded.csv",
    rows_per_batch: int = 200,
    query_batch_size: int = 50,
    require_all: bool = False,
) -> dict[str, Any]:
    """Read source CSV in bounded batches; write a retry-safe enriched blob."""
    if rows_per_batch < 1 or query_batch_size < 1:
        raise ValueError("rows_per_batch and query_batch_size must be positive")
    if not output_name or "/" in output_name or "\\" in output_name:
        raise ValueError("output_name must be a filename, not a path")

    prefix = str(destination.get("blob_prefix", "")).strip("/")
    blob_name = f"{prefix}/{output_name}" if prefix else output_name
    src = init_blob_client(blob_url=source_url)
    counts: Counter[str] = Counter()
    total_rows = 0
    try:
        with tempfile.TemporaryFile(mode="w+b") as temp:
            raw = _ChunksIO(src.download_blob().chunks())
            with io.TextIOWrapper(io.BufferedReader(raw), encoding="utf-8-sig", newline="") as text:
                reader = csv.DictReader(text)
                names = list(reader.fieldnames or [])
                if not names or len(names) != len(set(names)):
                    raise ValueError("Source CSV has missing or duplicate column headers")
                for extra in COORD_COLUMNS:
                    if extra not in names:
                        names.append(extra)
                _write_csv_rows(temp, names, [], header=True)

                while True:
                    batch: list[dict[str, Any]] = []
                    for _ in range(rows_per_batch):
                        try:
                            batch.append(next(reader))
                        except StopIteration:
                            break
                    if not batch:
                        break
                    enriched, batch_counts = enrich_coordinate_rows(
                        batch, batch_size=query_batch_size,
                    )
                    _write_csv_rows(temp, names, enriched, header=False)
                    counts.update(batch_counts)
                    total_rows += len(batch)

            missing = total_rows - counts["existing"] - counts["geocoded"]
            if require_all and missing:
                raise ValueError(
                    f"Coordinate enrichment unresolved for {missing}/{total_rows} rows: {dict(counts)}"
                )
            temp.seek(0)
            dest = init_blob_client(
                conn_str=destination["conn_str"],
                container_name=destination["container_name"],
                blob_name=blob_name,
            )
            try:
                dest.upload_blob(temp, overwrite=True)
                result_url = get_blob_sas(dest)
            finally:
                dest.close()
    finally:
        src.close()

    stats = dict(counts)
    logging.info(
        "Audience geocoding: rows=%d, unresolved=%d, statuses=%s, output=%s",
        total_rows, missing, stats, blob_name,
    )
    if missing:
        logging.warning(
            "Audience geocoding left %d/%d rows without usable coordinates. "
            "geocode_address no longer supplies accuracy/source classifications.",
            missing, total_rows,
        )
    return {"url": result_url, "blob_name": blob_name,
            "rows": total_rows, "unresolved": missing, "counts": stats}


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiences_geocodeMissingCoordinates(ingress: dict) -> dict[str, Any]:
    return enrich_coordinate_csv(
        source_url=ingress["source_url"],
        destination=ingress["destination"],
        output_name=ingress.get("output_name", "geocoded.csv"),
        rows_per_batch=int(ingress.get("rows_per_batch", os.getenv("AUDIENCE_COORD_ROWS_PER_BATCH", "200"))),
        query_batch_size=int(ingress.get("query_batch_size", os.getenv("AUDIENCE_COORD_SQL_BATCH_SIZE", "50"))),
        require_all=bool(ingress.get("require_all", False)),
    )

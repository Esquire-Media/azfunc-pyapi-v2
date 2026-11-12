"""Optional coordinates-only enrichment for address CSV blobs.

Call AFTER the existing AddrAll/autocomplete normalization when the next
operation needs latitude/longitude. This activity never changes address IDs,
norm_addy, or Smarty address components.
"""
from __future__ import annotations

import csv
import io
import logging
import os
import tempfile
from collections import Counter
from itertools import islice
from typing import Any, Mapping

from azure.durable_functions import Blueprint
from azure.storage.blob import ContentSettings

from libs.utils.address_coordinates import (
    COORD_COLUMNS,
    enrich_coordinate_rows,
    validate_options,
)
from libs.utils.azure_storage import get_blob_sas, get_cached_blob_client, init_blob_client

bp = Blueprint()


def _chunks(reader: csv.DictReader, size: int):
    while block := list(islice(reader, size)):
        yield block


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesNeighbors_geocodeAddressBlob(
    ingress: Mapping[str, Any],
) -> dict[str, Any]:
    """Enrich missing CSV coordinates; returns a new blob URL and status counts.

    Input: {source_url, destination:{conn_str,container_name,blob_prefix},
            source_index?, sources?, quality?, batch_size?, require_all?}

    quality=rooftop allows only stored address points (for tiny polygons).
    quality=radius also allows TIGER interpolated house-number points.
    quality=any accepts all geocoder sources, including street/locality.
    """
    source_url = str(ingress["source_url"])
    destination = ingress["destination"]
    index = int(ingress.get("source_index", 0))
    batch_size = int(ingress.get("batch_size", os.getenv("GEOCODE_COORD_BATCH_SIZE", "50")))
    sources = ingress.get("sources", ["usps", "openaddresses", "tiger"])
    quality = str(ingress.get("quality", "radius"))
    require_all = bool(ingress.get("require_all", False))
    validate_options(sources, quality, batch_size)

    blob_prefix = str(destination["blob_prefix"]).strip("/")
    name = f"{blob_prefix}/source-{index:05d}.csv" if blob_prefix else f"source-{index:05d}.csv"
    source_blob = get_cached_blob_client(source_url)
    dest_blob = init_blob_client(
        conn_str=destination.get("conn_str", "AzureWebJobsStorage"),
        container_name=destination["container_name"],
        blob_name=name,
    )

    counts: Counter[str] = Counter()
    with (tempfile.TemporaryFile(mode="w+b") as source_temp,
          tempfile.TemporaryFile(mode="w+b") as output_temp):
        source_blob.download_blob(max_concurrency=1).readinto(source_temp)
        source_temp.seek(0)
        # Hold the wrappers until the upload finishes; temporary files close
        # deterministically on Windows, even if geocoding fails.
        source_text = io.TextIOWrapper(source_temp, encoding="utf-8-sig", newline="")
        output_text = io.TextIOWrapper(output_temp, encoding="utf-8", newline="")
        reader = csv.DictReader(source_text)
        if not reader.fieldnames:
            raise ValueError("Input address CSV has no header")
        headers = list(dict.fromkeys([*reader.fieldnames, *COORD_COLUMNS]))
        writer = csv.DictWriter(output_text, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()

        # CSV is processed in bounded chunks. Rows retain input order and all
        # non-coordinate fields, including canonical UUID and norm_addy.
        for chunk in _chunks(reader, batch_size):
            enriched, result_counts = enrich_coordinate_rows(
                chunk, sources=sources, quality=quality, batch_size=batch_size,
            )
            writer.writerows(enriched)
            counts.update(result_counts)
        output_text.flush()
        unresolved = sum(count for state, count in counts.items()
                         if state not in {"existing", "geocoded"})
        logging.info(
            "[AudienceLatLon] input=%s output=%s counts=%s unresolved=%d",
            source_url.split("?")[0], name, dict(counts), unresolved,
        )
        if unresolved:
            logging.warning(
                "[AudienceLatLon] %d rows remain without usable coordinates; "
                "downstream spatial filters cannot include those rows. status_counts=%s",
                unresolved, dict(counts),
            )
        if require_all and unresolved:
            raise ValueError(f"Coordinate enrichment left {unresolved} rows unresolved: {dict(counts)}")
        output_temp.seek(0)
        dest_blob.upload_blob(
            output_temp, overwrite=True, max_concurrency=1,
            content_settings=ContentSettings(content_type="text/csv"),
        )

    return {
        "url": get_blob_sas(dest_blob),
        "rows": sum(counts.values()),
        "geocoded": counts["geocoded"],
        "existing": counts["existing"],
        "unresolved": unresolved,
        "status_counts": dict(counts),
    }

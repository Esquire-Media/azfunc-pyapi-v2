"""Compatibility activity for the retired geocodeAddressBlob entry point.

The maintained implementation is geocodeMissingCoordinates.enrich_coordinate_csv.
Both Azure activities use the same coordinate-only enrichment, with no changes
 to AddrAll, canonical UUIDs, or normalized address components.
"""
from __future__ import annotations

import logging
from typing import Any, Mapping

from azure.durable_functions import Blueprint

from libs.azure.functions.blueprints.esquire.audiences.utils.activities.geocodeMissingCoordinates import (
    enrich_coordinate_csv,
)

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesNeighbors_geocodeAddressBlob(
    ingress: Mapping[str, Any],
) -> dict[str, Any]:
    """Preserve the old activity name and result keys for existing orchestrators."""
    if "sources" in ingress or "quality" in ingress:
        logging.warning(
            "[AudienceLatLon] Legacy sources/quality options are ignored: "
            "addresses.geocode_address no longer reports source or match type."
        )

    index = int(ingress.get("source_index", 0))
    result = enrich_coordinate_csv(
        source_url=str(ingress["source_url"]),
        destination=dict(ingress["destination"]),
        output_name=str(ingress.get("output_name", f"source-{index:05d}.csv")),
        rows_per_batch=int(ingress.get("rows_per_batch", 200)),
        query_batch_size=int(ingress.get("batch_size", 50)),
        require_all=bool(ingress.get("require_all", False)),
    )
    counts = result["counts"]
    return {
        **result,
        "geocoded": counts.get("geocoded", 0),
        "existing": counts.get("existing", 0),
        "status_counts": counts,
    }

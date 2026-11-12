from __future__ import annotations

import hashlib
import os
from typing import Any, Iterator, TypeVar

from azure.durable_functions import (
    Blueprint,
    DurableOrchestrationContext,
    RetryOptions,
)


bp = Blueprint()

T = TypeVar("T")


_MAX_CONCURRENT_NORMALIZATIONS = int(
    os.getenv(
        "ADDRESS_NORMALIZATION_MAX_CONCURRENCY",
        "3",
    )
)

_MAX_CONCURRENT_SPLITS = int(
    os.getenv("ADDRESS_NORMALIZATION_MAX_CONCURRENT_SPLITS", "2")
)

_ROWS_PER_ACTIVITY = int(
    os.getenv("ADDRESS_NORMALIZATION_ROWS_PER_ACTIVITY", "500")
)


_ACTIVITY_RETRY_ATTEMPTS = int(
    os.getenv(
        "ADDRESS_NORMALIZATION_ACTIVITY_RETRY_ATTEMPTS",
        "3",
    )
)

_ACTIVITY_RETRY_DELAY_MS = int(
    os.getenv(
        "ADDRESS_NORMALIZATION_ACTIVITY_RETRY_DELAY_MS",
        "5000",
    )
)


def _chunked(
    items: list[T],
    size: int,
) -> Iterator[list[T]]:
    if size <= 0:
        raise ValueError(
            "Chunk size must be > 0"
        )

    for start in range(
        0,
        len(items),
        size,
    ):
        yield items[
            start:start + size
        ]


@bp.orchestration_trigger(
    context_name="context"
)
def orchestrator_esquireAudiencesNeighbors_normalizeAddresses(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input() or {}

    source_urls = [
        str(url)
        for url in ingress.get(
            "source_urls",
            [],
        )
        if url
    ]

    if not source_urls:
        return {
            "results": [],
            "transformed": False,
        }

    retry = RetryOptions(
        _ACTIVITY_RETRY_DELAY_MS,
        _ACTIVITY_RETRY_ATTEMPTS,
    )

    if _ROWS_PER_ACTIVITY < 1:
        raise ValueError("ADDRESS_NORMALIZATION_ROWS_PER_ACTIVITY must be positive")

    # All chunk names are deterministic within this orchestration instance.
    # Do not put SAS-bearing source URLs into blob names or logs.
    run_key = hashlib.sha256(context.instance_id.encode("utf-8")).hexdigest()[:20]
    working = ingress.get("working")
    if not isinstance(working, dict) or not all(
        key in working for key in ("conn_str", "container_name")
    ):
        working = ingress["destination"]
    prefix = str(working.get("blob_prefix", "")).strip("/")
    chunk_prefix = "/".join(
        part for part in (prefix, "address-normalization-input", run_key) if part
    )
    chunk_destination = {**working, "blob_prefix": chunk_prefix}

    chunk_urls: list[str] = []
    for group in _chunked(list(enumerate(source_urls)), _MAX_CONCURRENT_SPLITS):
        tasks = [
            context.call_activity_with_retry(
                "activity_esquireAudiencesNeighbors_splitAddressBlob",
                retry,
                {
                    "source_index": source_index,
                    "source_url": source_url,
                    "destination": chunk_destination,
                    "rows_per_chunk": _ROWS_PER_ACTIVITY,
                },
            )
            for source_index, source_url in group
        ]
        for result in (yield context.task_all(tasks)):
            chunk_urls.extend(str(url) for url in result["urls"])

    # Existing normalization activity and concurrency limit are unchanged.
    # Its source files now contain at most _ROWS_PER_ACTIVITY CSV rows.
    indexed_sources = list(
        enumerate(chunk_urls)
    )

    output_urls: list[str] = []
    transformed = False

    for group in _chunked(
        indexed_sources,
        _MAX_CONCURRENT_NORMALIZATIONS,
    ):
        tasks = [
            context.call_activity_with_retry(
                "activity_esquireAudiencesNeighbors_normalizeAddressBlob",
                retry,
                {
                    **ingress,
                    "source_index": source_index,
                    "source_url": source_url,
                },
            )
            for (
                source_index,
                source_url,
            ) in group
        ]

        results: list[
            dict[str, Any]
        ] = yield context.task_all(
            tasks
        )

        output_urls.extend(
            str(result["url"])
            for result in results
            if result.get("url")
        )

        transformed = (
            transformed
            or any(
                bool(
                    result.get(
                        "transformed",
                        False,
                    )
                )
                for result in results
            )
        )

    return {
        "results": output_urls,
        "transformed": transformed,
    }
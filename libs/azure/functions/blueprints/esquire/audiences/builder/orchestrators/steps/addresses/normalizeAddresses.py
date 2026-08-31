from __future__ import annotations

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

    indexed_sources = list(
        enumerate(source_urls)
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
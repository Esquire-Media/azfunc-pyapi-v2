import os
from typing import Iterator, TypeVar

from azure.durable_functions import (
    Blueprint,
    DurableOrchestrationContext,
    RetryOptions,
)


bp = Blueprint()

T = TypeVar("T")


_PARTITIONS_PER_ACTIVITY = int(
    os.getenv(
        "NEIGHBORS_PARTITIONS_PER_ACTIVITY",
        "5",
    )
)

_MAX_CONCURRENT_BATCHES = int(
    os.getenv(
        "NEIGHBORS_MAX_CONCURRENT_BATCHES",
        "5",
    )
)

_ACTIVITY_RETRY_ATTEMPTS = int(
    os.getenv(
        "NEIGHBORS_ACTIVITY_RETRY_ATTEMPTS",
        "3",
    )
)

_ACTIVITY_RETRY_DELAY_MS = int(
    os.getenv(
        "NEIGHBORS_ACTIVITY_RETRY_DELAY_MS",
        "5000",
    )
)


def _chunked(
    items: list[T],
    size: int,
) -> Iterator[list[T]]:
    if size <= 0:
        raise ValueError("Chunk size must be > 0")

    for start in range(0, len(items), size):
        yield items[start:start + size]


@bp.orchestration_trigger(
    context_name="context"
)
def orchestrator_esquireAudiencesSteps_addresses2neighbors(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input() or {}

    retry = RetryOptions(
        _ACTIVITY_RETRY_DELAY_MS,
        _ACTIVITY_RETRY_ATTEMPTS,
    )

    partitions = yield context.call_activity_with_retry(
        "activity_esquireAudiencesNeighbors_extractPartitions",
        retry,
        ingress,
    )

    if not partitions:
        return []

    run_id = context.instance_id

    # Batch indexes are assigned globally before concurrency waves so
    # retries/waves always target the same result blob names.
    batches = list(
        enumerate(
            _chunked(
                partitions,
                _PARTITIONS_PER_ACTIVITY,
            )
        )
    )

    out_urls: list[str] = []

    for batch_group in _chunked(
        batches,
        _MAX_CONCURRENT_BATCHES,
    ):
        tasks = [
            context.call_activity_with_retry(
                "activity_esquireAudiencesNeighbors_processBatch_blockblob",
                retry,
                {
                    **ingress,
                    "run_id": run_id,
                    "batch_index": batch_index,
                    "partitions": batch,
                },
            )
            for batch_index, batch in batch_group
        ]

        results = yield context.task_all(tasks)

        out_urls.extend(
            result
            for result in results
            if result
        )

    return out_urls

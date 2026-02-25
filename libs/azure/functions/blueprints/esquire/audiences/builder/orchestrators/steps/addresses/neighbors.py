import os
from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions

bp = Blueprint()

# How many partitions are processed inside ONE activity
_PARTITIONS_PER_ACTIVITY = int(os.getenv("NEIGHBORS_PARTITIONS_PER_ACTIVITY", "100"))

# How many batch activities can run concurrently
_MAX_CONCURRENT_BATCHES = int(os.getenv("NEIGHBORS_MAX_CONCURRENT_BATCHES", "15"))


def _chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_addresses2neighbors(context: DurableOrchestrationContext):
    ingress = context.get_input() or {}

    retry = RetryOptions(first_retry_interval_in_milliseconds=5000, max_number_of_attempts=3)

    # Keep your existing extractor (or swap to your upgraded one); it returns a list of partitions.
    partitions = yield context.call_activity_with_retry(
        "activity_esquireAudiencesNeighbors_extractPartitions",
        retry,
        ingress,
    )

    if not partitions:
        return []

    run_id = context.instance_id

    # Create batch work items
    batches = []
    batch_index = 0
    for part_batch in _chunked(partitions, _PARTITIONS_PER_ACTIVITY):
        batches.append(
            {
                "run_id": run_id,
                "batch_index": batch_index,
                "partitions": part_batch,
                "source_urls": ingress.get("source_urls", []),
                "destination": ingress["destination"],
                "process": ingress.get("process", {}),
                # optional override
                "db_bind": ingress.get("db_bind", "keystone"),
            }
        )
        batch_index += 1

    # Run batch activities with bounded concurrency
    out_urls: list[str] = []
    for batch_group in _chunked(batches, _MAX_CONCURRENT_BATCHES):
        tasks = [
            context.call_activity_with_retry(
                "activity_esquireAudiencesNeighbors_processPartitionBatch_blockblob",
                retry,
                b,
            )
            for b in batch_group
        ]
        results = yield context.task_all(tasks)
        out_urls.extend([r for r in results if r])

    return out_urls
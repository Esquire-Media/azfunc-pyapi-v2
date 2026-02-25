import os
from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions

bp = Blueprint()

_PARTITIONS_PER_ACTIVITY = int(os.getenv("NEIGHBORS_PARTITIONS_PER_ACTIVITY", "100"))
_MAX_CONCURRENT_BATCHES = int(os.getenv("NEIGHBORS_MAX_CONCURRENT_BATCHES", "15"))

def _chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_addresses2neighbors(context: DurableOrchestrationContext):

    ingress = context.get_input() or {}
    retry = RetryOptions(5000, 3)

    partitions = yield context.call_activity_with_retry(
        "activity_esquireAudiencesNeighbors_extractPartitions",
        retry,
        ingress,
    )

    if not partitions:
        return []

    run_id = context.instance_id
    out_urls: list[str] = []

    batches = list(_chunked(partitions, _PARTITIONS_PER_ACTIVITY))

    for batch_group in _chunked(batches, _MAX_CONCURRENT_BATCHES):
        tasks = [
            context.call_activity_with_retry(
                "activity_esquireAudiencesNeighbors_processBatch_blockblob",
                retry,
                {
                    **ingress,
                    "run_id": run_id,
                    "batch_index": idx,
                    "partitions": batch,
                },
            )
            for idx, batch in enumerate(batch_group)
        ]

        results = yield context.task_all(tasks)
        out_urls.extend([r for r in results if r])

    return out_urls
# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/steps/addresses/device_ids.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
# import logging
bp = Blueprint()

MAX_CONCURRENT_TASKS = 10


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_addresses2neighbors(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()

    # Optional but recommended: retries smooth over transient DB/network errors
    retry = RetryOptions(first_retry_interval_in_milliseconds=5000, max_number_of_attempts=3)

    partition_keys = yield context.call_activity_with_retry(
        "activity_esquireAudiencesNeighbors_extractPartitions",
        retry,
        ingress,
    )

    # Step 2: Query 
    # logging.warning(f"[LOG] Setting {len(partition_keys)} tasks")
    out_urls: list[str] = []
    
    # Process partitions in batches; write results per-batch; don't accumulate all_results
    for parts_batch in chunked(partition_keys, MAX_CONCURRENT_TASKS):
        neighbor_tasks = [
            context.call_activity_with_retry(
                "activity_esquireAudiencesNeighbors_findNeighbors",
                retry,
                {
                    "city": part["city"],
                    "state": part["state"],
                    "zip": part["zip"],
                    "n_per_side": ingress.get("process", {}).get("housesPerSide", 20),
                    "same_side_only": ingress.get("process", {}).get("bothSides", True),
                    "source_urls": ingress.get("source_urls", []),
                },
            )
            for part in parts_batch
        ]

        batch_results = yield context.task_all(neighbor_tasks)

        # Only write non-empty results, and throttle writes too
        write_tasks = [
            context.call_activity(
                "activity_write_blob",
                {
                    "records": recs,
                    "container": ingress["destination"]["container_name"],
                    "blob_prefix": f"{ingress['destination']['blob_prefix']}",
                    "conn_str": "AzureWebJobsStorage",
                    "preflight": True,
                },
            )
            for recs in batch_results
            if recs  # skips None and []
        ]

        for write_batch in chunked(write_tasks, MAX_CONCURRENT_TASKS):
            out_urls.extend((yield context.task_all(write_batch)))

    return out_urls

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]
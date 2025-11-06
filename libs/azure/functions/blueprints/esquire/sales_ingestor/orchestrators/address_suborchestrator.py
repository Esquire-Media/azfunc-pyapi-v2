from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
import os
import logging

logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.WARNING)

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def suborchestrator_salesIngestor_enrichAddresses(context: DurableOrchestrationContext):
    """
    Input includes:
      - scope: 'billing' | 'shipping'
      - staging_table
      - parallelism (optional)
      - plus all original settings
    """
    settings = context.get_input()
    retry = RetryOptions(15000, 3)

    # Build a stable plan (no shared temp tables)
    plan = yield context.call_activity_with_retry(
        "activity_salesIngestor_planAddressBatches",
        retry,
        settings
    )
    ranges = plan["ranges"]
    # Pull dynamically determined parallelism from plan result
    parallelism = plan.get("suggested_parallelism") or int(
        settings.get("parallelism", os.getenv("ADDRESS_PARALLELISM", 10))
    )

    results = []
    i = 0
    total = len(ranges)
    while i < total:
        window = ranges[i:i+parallelism]
        tasks = [
            context.call_activity_with_retry(
                "activity_salesIngestor_enrichAddresses_batch",
                retry,
                {
                    "scope": settings["scope"],
                    "staging_table": settings["staging_table"],
                    "range": r,
                    "fields": settings["fields"],
                    "metadata": settings["metadata"],
                    "tenant_id": settings.get("tenant_id"),
                }
            )
            for r in window
        ]
        batch_results = yield context.task_all(tasks)
        results.extend(batch_results)
        i += parallelism

    return results

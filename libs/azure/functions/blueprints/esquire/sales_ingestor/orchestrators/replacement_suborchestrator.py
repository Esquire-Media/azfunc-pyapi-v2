"""Post-ingestion suborchestrator for replacing old sales batches."""

from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def suborchestrator_salesIngestor_replaceBatches(
    context: DurableOrchestrationContext,
):
    settings = context.get_input()
    retry = RetryOptions(15000, 3)

    # Keep all destructive SQL in ONE activity/transaction so Azure Durable
    # retries cannot expose a partially cleaned up replacement.
    result = yield context.call_activity_with_retry(
        "activity_salesIngestor_applyReplacement",
        retry,
        settings,
    )
    return result

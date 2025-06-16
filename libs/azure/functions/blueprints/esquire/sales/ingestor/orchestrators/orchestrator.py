from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_ingestData(context: DurableOrchestrationContext):
    payload = context.get_input()
    retry = RetryOptions(15000, 3)

    blob_data = yield context.call_activity_with_retry("activity_readBlob", retry, payload["blob_id"])
    validated = yield context.call_activity_with_retry("activity_validateAddresses", retry, blob_data)
    transformed = yield context.call_activity_with_retry("activity_transformData", retry, {
        "fields": payload["fields"],
        "metadata": payload["metadata"],
        "data": validated,
    })
    yield context.call_activity_with_retry("activity_writeDatabase", retry, transformed)
    return {"status": "success"}

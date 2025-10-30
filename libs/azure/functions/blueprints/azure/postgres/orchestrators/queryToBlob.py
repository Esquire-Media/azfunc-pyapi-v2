from azure.durable_functions import Blueprint, DurableOrchestrationContext

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_azurePostgres_queryToBlob(context: DurableOrchestrationContext):
    # ingress = {
    #     "source": {
    #         "bind": "BIND_HANDLE",
    #         "query": "SELECT * FROM table",
    #     },
    #     "destination": {
    #         "conn_str": "YOUR_AZURE_CONNECTION_STRING_ENV_VARIABLE",
    #         "container_name": "your-azure-blob-container",
    #         "blob_prefix": "blob/prefix",
    #         "format": "CSV",
    #     },
    #     "limit": 1000
    # }
    ingress: dict = context.get_input() or {}

    count = yield context.call_activity(
        "activity_azurePostgres_getRecordCount",
        ingress["source"],
    )

    # Ensure a sane, positive limit
    raw_limit = ingress.get("limit", 1000)
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        limit = 1000
    if limit <= 0:
        limit = 1000

    dest = ingress["destination"]
    prefix = dest["blob_prefix"]
    ext = dest["format"].lower()

    # Build deterministic work items with precomputed blob names
    tasks = []
    for offset in range(0, count, limit):
        blob_name = f"{prefix}/offset-{offset}.{ext}"
        payload = {
            "source": ingress["source"],
            "destination": {**dest, "blob_name": blob_name},
            "limit": limit,
            "offset": offset,
        }
        tasks.append(
            context.call_activity(
                "activity_azurePostgres_resultToBlob",
                payload,
            )
        )

    if not tasks:
        # No rows matched, nothing to upload
        return []

    # Each activity returns: {"offset", "blob_name", "url"}
    results = yield context.task_all(tasks)
    return results

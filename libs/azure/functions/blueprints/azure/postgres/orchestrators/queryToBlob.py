# File: /libs/azure/functions/blueprints/azure/postgres/queryToBlob.py

from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions import Blueprint

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
    #     }
    # }
    ingress = context.get_input()
    count = yield context.call_activity(
        "activity_azurePostgres_getRecordCount", ingress["source"]
    )
    limit = ingress.get("limit", 1000)
    urls = yield context.task_all(
        [
            context.call_activity(
                "activity_azurePostgres_resultToBlob",
                {**ingress, "limit": limit, "offset": i},
            )
            for i in range(0, count, limit)
        ]
    )
    return urls

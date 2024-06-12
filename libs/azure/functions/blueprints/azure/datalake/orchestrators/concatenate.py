# File: /libs/azure/functions/blueprints/azure/datalake/concatenate.py

from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions import Blueprint
from libs.utils.azure_storage import get_blob_sas, init_blob_client

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_azureDatalake_concatenate(context: DurableOrchestrationContext):
    block_ids = yield context.task_all(
        [
            context.call_activity(
                "",
                {
                    "index": index,
                    "source": source,
                    "destination": context.get_input("destination"),
                },
            )
            for index, source in enumerate(context.get_input("sources"))
        ]
    )

    destination = init_blob_client(**context.get_input("destination"))
    destination.commit_block_list(
        [
            block_id
            for item in sorted(block_ids, key=lambda x: x["index"])
            for block_id in item["block_ids"]
        ]
    )

    return get_blob_sas(destination)

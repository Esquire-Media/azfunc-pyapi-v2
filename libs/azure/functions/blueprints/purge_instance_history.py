# File: libs/azure/functions/blueprints/purge_instance_history.py

from azure.durable_functions import (
    DurableOrchestrationClient,
    DurableOrchestrationContext,
)
from azure.functions import TimerRequest
from azure.data.tables import TableClient
from libs.azure.functions import Blueprint
from libs.azure.functions.suborchestrators import get_sub_orchestrator_ids
import os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def get_sub_orchestrator_ids_activity(ingress: dict):
    return get_sub_orchestrator_ids(
        TableClient.from_connection_string(
            conn_str=os.environ[ingress.get("conn_str", "AzureWebJobsStorage")],
            table_name=ingress.get("task_hub_name", os.environ["TASK_HUB_NAME"])
            + "Instances",
        ),
        ingress["instance_id"],
    )


@bp.activity_trigger(input_name="instanceId")
@bp.durable_client_input(client_name="client")
async def purge_instance_history_activity(
    instanceId: str, client: DurableOrchestrationClient
):
    await client.purge_instance_history(instanceId)
    return ""


@bp.orchestration_trigger(context_name="context")
def purge_instance_history(context: DurableOrchestrationContext):
    ingress: dict = context.get_input()
    # Check for sub-instances
    sub_instance_ids = yield context.call_activity(
        "get_sub_orchestrator_ids_activity", ingress
    )

    ## Purge sub-instances
    yield context.task_all(
        [
            context.call_sub_orchestrator(
                "purge_instance_history",
                {
                    **ingress,
                    "instance_id": instance_id,
                },
            )
            for instance_id in sub_instance_ids
            if instance_id != context.instance_id
        ]
    )
    context.set_custom_status("sub-instances purged")

    # Purge Self
    instance_ids = yield context.call_activity(
        "get_sub_orchestrator_ids_activity", {"instance_id": context.instance_id}
    )
    yield context.task_all(
        [
            context.call_activity(
                "purge_instance_history_activity",
                instance_id,
            )
            for instance_id in instance_ids
        ]
    )
    context.set_custom_status("self sub-instances purged")

    ## Purge
    yield context.call_activity(
        "datalake_activity_delete_directory",
        {
            **ingress,
            "container_name": ingress.get("task_hub_name", os.environ["TASK_HUB_NAME"])
            + "-largemessages",
            "prefix": context.instance_id,
        },
    )
    context.set_custom_status("target data purged")
    yield context.call_activity(
        "purge_instance_history_activity",
        ingress["instance_id"],
    )
    context.set_custom_status("target purged")

    return ""


@bp.timer_trigger("timer", schedule="0 */5 * * * *")
@bp.durable_client_input("client")
async def daily_dashboard_xandr_starter(
    timer: TimerRequest, client: DurableOrchestrationClient
):
    for entity in TableClient.from_connection_string(
        conn_str=os.environ["AzureWebJobsStorage"],
        table_name=os.environ["TASK_HUB_NAME"] + "Instances",
    ).query_entities(
        query_filter="Name eq 'purge_instance_history' and RuntimeStatus eq 'Completed'",
        select=["PartitionKey"],
    ):
        await client.purge_instance_history(entity["PartitionKey"])

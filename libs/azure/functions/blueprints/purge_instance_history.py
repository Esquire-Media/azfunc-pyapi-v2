# File: libs/azure/functions/blueprints/purge_instance_history.py

from azure.durable_functions import DurableOrchestrationClient
from azure.data.tables import TableClient
from libs.azure.functions import Blueprint
import os

bp = Blueprint()


def get_sub_orchestrator_ids(table: TableClient, parent_instance_id: str):
    """
    Retrieve sub-orchestrator IDs associated with a parent orchestrator instance.

    Parameters
    ----------
    table : TableClient
        Azure Table Client instance to interact with Azure Table Storage.
    parent_instance_id : str
        ID of the parent orchestrator instance.

    Returns
    -------
    list
        List of sub-orchestrator IDs.
    """
    entities = list(
        table.query_entities(
            f"PartitionKey eq '{parent_instance_id}'", select=["ExecutionId"]
        )
    )

    if not entities:
        return []

    ids = []
    for e in entities:
        for i in table.query_entities(
            "PartitionKey ge '{}' and PartitionKey le '{}'".format(
                e["ExecutionId"] + ":",
                e["ExecutionId"] + chr(ord(":") + 1),
            ),
            select=["PartitionKey"],
        ):
            ids.append(i["PartitionKey"])
            ids.extend(get_sub_orchestrator_ids(table, i["PartitionKey"]))

    return ids


@bp.activity_trigger(input_name="ingress")
@bp.durable_client_input(client_name="client")
async def purge_instance_history(ingress: dict, client: DurableOrchestrationClient):
    """
    Purge history of a durable orchestration instance and its sub-orchestrators.

    This function purges the history of a given durable orchestration instance
    and all associated sub-orchestrators' histories.

    Parameters
    ----------
    ingress : dict
        Dictionary containing details about the connection string, task hub name, and instance ID.
    client : DurableOrchestrationClient
        Azure Durable Functions client to interact with Durable Functions extension.

    Returns
    -------
    str
        An empty string indicating completion.
    """

    # Initialize Azure Table client to interact with Azure Table Storage
    table = TableClient.from_connection_string(
        conn_str=os.getenv(
            ingress.get("conn_str", ""), os.environ["AzureWebJobsStorage"]
        ),
        table_name=ingress.get("task_hub_name", os.environ["TASK_HUB_NAME"])
        + "Instances",
    )

    # Purge the history for each sub-orchestrator associated with the main instance
    for instance_id in get_sub_orchestrator_ids(table, ingress["instance_id"]):
        await client.purge_instance_history(instance_id=instance_id)

    # Purge the history of the main orchestration instance
    await client.purge_instance_history(instance_id=ingress["instance_id"])
    return ""

# File: libs/azure/functions/blueprints/purge_instance_history.py

from azure.durable_functions import DurableOrchestrationClient
from azure.data.tables import TableClient
from azure.storage.filedatalake import FileSystemClient
from libs.azure.functions import Blueprint
from libs.azure.functions.suborchestrators import get_sub_orchestrator_ids
import os

bp = Blueprint()


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

    conn_str = os.environ[ingress.get("conn_str", "AzureWebJobsStorage")]

    # Initialize Azure Table client to interact with Azure Table Storage
    table = TableClient.from_connection_string(
        conn_str=conn_str,
        table_name=ingress.get("task_hub_name", os.environ["TASK_HUB_NAME"])
        + "Instances",
    )

    filesystem = FileSystemClient.from_connection_string(
        conn_str=conn_str,
        file_system_name=ingress.get("task_hub_name", os.environ["TASK_HUB_NAME"])
        + "-largemessages",
    )

    # Purge the history for each sub-orchestrator associated with the main instance
    for instance_id in get_sub_orchestrator_ids(table, ingress["instance_id"]):
        for item in filesystem.get_paths(recursive=False):
            if item["is_directory"] and item["name"].startswith(instance_id):
                filesystem.get_directory_client(item).delete_directory()
        tries = 0
        while True:
            if tries > 3:
                break
            try:
                await client.purge_instance_history(instance_id=instance_id)
                break
            except:
                tries += 1

    # Purge the history of the main orchestration instance
    for item in filesystem.get_paths(recursive=False):
        if item["is_directory"] and item["name"].startswith(ingress["instance_id"]):
            filesystem.get_directory_client(item).delete_directory()
    await client.purge_instance_history(instance_id=ingress["instance_id"])
    return ""

# File: libs/azure/functions/blueprints/esquire/audiences/mover_sync/orchestrators/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.movers_sync.cetas import (
    create_cetas_query,
)
import uuid
import os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_moversSync_root(context: DurableOrchestrationContext):
    """
    Orchestrator function for the Movers Sync process in Esquire Audiences.

    Coordinates tasks for copying files from S3 to Azure, validating addresses,
    updating Synapse database tables, and cleaning up old data. It also handles
    the creation of CETAS tables for new or updated datasets.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context for the orchestration, containing methods for interacting with
        the Durable Functions runtime.
    """
    try:
        retry = RetryOptions(15000, 1)

        egress = {
            "runtime_container": {
                "conn_str": "AzureWebJobsStorage",
                "container_name": "mover-data",
            },
            "rowCounts_table": {
                "conn_str": "AzureWebJobsStorage",
                "table_name": "rowCounts",
            },
        }

        # get list of files to copy from S3 to Azure
        files_to_copy = yield context.call_activity_with_retry(
            "activity_moversSync_getFileList", retry, egress
        )

        # NOTE Use for local testing only!
        # files_to_copy = []

        # if there are new files, copy them over and store their row count in the table
        if len(files_to_copy):
            # create a CopyFile activity for each file that needs to be copied
            azure_paths = yield context.task_all(
                [
                    context.call_activity_with_retry(
                        "activity_moversSync_copyFile",
                        retry,
                        {
                            **egress,
                            "filepath": file,
                        },
                    )
                    for file in files_to_copy
                ]
            )

            # get the row count of each copied file, and store that info in the rowCounts table
            yield context.task_all(
                [
                    context.call_activity_with_retry(
                        "activity_moversSync_getBlobRowCount",
                        retry,
                        {
                            **egress,
                            "blob_name": f"{egress['runtime_container']['container_name']}/{azure_path}",
                        },
                    )
                    for azure_path in azure_paths
                ]
            )

        # start a sub orchestrator that checks if anything still needs to be address validated, and validate it if so
        # do this every runtime, not just when a new file is moved over
        affected_datasets = yield context.call_sub_orchestrator_with_retry(
            "orchestrator_moversSync_validateAddresses",
            retry,
            {
                **egress,
            },
        )

        for blob_type in affected_datasets:
            # identify existing tables which will need to be dropped once the new one is created
            old_tables = yield context.call_activity_with_retry(
                "activity_synapse_queryTables",
                retry,
                {"bind": "audiences", "pattern": f"{blob_type}_%"},
            )

            # for each dataset where data was added/validated, re-create its CETAS table
            yield context.call_activity_with_retry(
                "synapse_activity_cetas",
                retry,
                {
                    "instance_id": context.instance_id,
                    "bind": "audiences",
                    "table": {"name": blob_type},
                    "destination": {
                        "conn_str": egress["runtime_container"]["conn_str"],
                        "container_name": egress["runtime_container"]["container_name"],
                        "handle": "sa_esquiremovers",
                        "path": f"{blob_type}-cetas/{uuid.uuid4().hex}",
                        "format": "PARQUET",
                    },
                    "query": create_cetas_query(blob_type=blob_type),
                    "commit": True,
                    "view": True,
                },
            )

            # drop the tables which were marked for deletion earlier
            yield context.call_activity(
                "activity_synapse_cleanupTables",
                {"bind": "audiences", "table_names": old_tables, "schema": "dbo"},
            )

        # Purge history related to this instance
        yield context.call_sub_orchestrator(
            "purge_instance_history",
            {"instance_id": context.instance_id},
        )

    except Exception as e:
        # if any errors are caught, post an error card to teams tagging Ryan
        yield context.call_activity(
            "activity_microsoftGraph_postErrorCard",
            {
                "function_name": "esquire-movers-sync",
                "instance_id": context.instance_id,
                "owners":["66c0c96a-2319-494e-a3a3-bc9c1b92739d"],
                "error": f"{type(e).__name__} : {e}"[:1000],
                "webhook": os.environ["EXCEPTIONS_WEBHOOK_DEVOPS"],
            },
        )
        raise e

    return {}
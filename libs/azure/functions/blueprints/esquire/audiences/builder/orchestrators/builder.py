# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/builder.py

from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions import Blueprint
from dateutil.relativedelta import relativedelta
import os, datetime

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_builder(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the building of Esquire audiences.

    This orchestrator performs several steps to process audience data, including fetching audience details, generating primary datasets, running processing steps, finalizing data, and pushing the data to configured DSPs.

    Parameters:
    context (DurableOrchestrationContext): The context object provided by Azure Durable Functions, used to manage and track the orchestration.

    Returns:
    dict: The final state of the ingress data after processing.

    Expected format for context.get_input():
    {
        "working": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "destination": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "audience": {
            "id": str,
            "rebuildRequired": bool,
        },
    }
    """

    try:
        # Retrieve the input data for the orchestration
        ingress = context.get_input()
        ingress["instance_id"] = context.instance_id

        # Fetch the full details for the audience
        ingress["audience"] = yield context.call_activity(
            "activity_esquireAudienceBuilder_fetchAudience",
            ingress["audience"],
        )

        rebuild = ingress["audience"].get("rebuildRequired", False)
        if not rebuild:
            audience_blob_prefix = yield context.call_activity(
                "activity_esquireAudiencesUtils_newestAudienceBlobPrefix",
                {
                    "conn_str": ingress["destination"]["conn_str"],
                    "container_name": ingress["destination"]["container_name"],
                    "audience_id": ingress["audience"]["id"],
                },
            )
            if not audience_blob_prefix:
                rebuild = True
            else:
                if context.current_utc_datetime > (
                    datetime.datetime.fromisoformat(audience_blob_prefix.split("/")[-1])
                    + relativedelta(
                        **{
                            ingress["audience"]["rebuildUnit"]: ingress["audience"][
                                "rebuild"
                            ]
                        }
                    )
                ):
                    rebuild = True

        if rebuild:
            # Check if the audience has a data source
            if ingress["audience"].get("dataSource"):
                # Generate a primary data set
                ingress = yield context.call_sub_orchestrator(
                    "orchestrator_esquireAudiences_primaryData", ingress
                )

                # Run processing steps
                ingress = yield context.call_sub_orchestrator(
                    "orchestrator_esquireAudiences_processingSteps", ingress
                )

                # Ensure Device IDs are the final data type
                ingress = yield context.call_sub_orchestrator(
                    "orchestrator_esquireAudiences_finalize", ingress
                )

                # Push the newly generated audiences to the DSPs that are configured
                tasks = []
                if ingress["audience"]["advertiser"]["meta"]:
                    tasks.append(
                        context.call_sub_orchestrator(
                            "meta_customaudience_orchestrator",
                            ingress,
                        )
                    )
                if ingress["audience"]["advertiser"]["xandr"]:
                    tasks.append(
                        context.call_sub_orchestrator(
                            "xandr_segment_orchestrator",
                            ingress,
                        )
                    )

                # Wait for all tasks to complete
                yield context.task_all(tasks)
                
        # Purge history related to this instance
        yield context.call_sub_orchestrator(
            "purge_instance_history",
            {"instance_id": context.instance_id},
        )

    except Exception as e:
        # if any errors are caught, post an error card to teams tagging Ryan and the calling user
        yield context.call_activity(
            "activity_microsoftGraph_postErrorCard",
            {
                "function_name": "esquire-auto-audience",
                "instance_id": context.instance_id,
                "owners": ["8489ce7c-e89f-4710-9d34-1442684ce7fe"],
                "error": f"{ingress['audience']['id']}: {type(e).__name__} : {e}"[
                    :1000
                ],
                "webhook": os.environ["EXCEPTIONS_WEBHOOK_DEVOPS"],
            },
        )
        raise e

    # Return the final state of the ingress data
    return ingress

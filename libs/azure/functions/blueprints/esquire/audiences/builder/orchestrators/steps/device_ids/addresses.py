# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/steps/device_ids/addresses.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
import uuid

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_deviceids2addresses(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the conversion of device IDs to addresses for Esquire audiences.

    This orchestrator processes device IDs to generate corresponding addresses and returns the URLs of the processed data.

    Parameters:
    context (DurableOrchestrationContext): The context object provided by Azure Durable Functions, used to manage and track the orchestration.

    Returns:
    list: The URLs of the processed data results.

    Expected format for context.get_input():
    {
        "working": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "source_urls": [str]
    }
    """

    ingress = context.get_input()

    # Convert device IDs to addresses
    onspot = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "onspot_orchestrator",
                {
                    **ingress["working"],
                    "endpoint": "/save/files/household",
                    "request": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Files",
                                "paths": [source_url.replace("https://", "az://")],
                                "properties": {
                                    "name": context.new_uuid(),
                                    "fileName": context.new_uuid() + ".csv",
                                    "hash": False,
                                    "fileFormat": {
                                        "delimiter": ",",
                                        "quoteEncapsulate": True,
                                    },
                                },
                            }
                        ],
                    },
                },
            )
            for source_url in ingress["source_urls"]
        ]
    )

    # Collect URLs of the converted results
    source_urls = []
    for result in onspot:
        job_location_map = {
            job["id"]: job["location"].replace("az://", "https://")
            for job in result["jobs"]
        }
        for callback in result["callbacks"]:
            if callback["success"]:
                if callback["id"] in job_location_map:
                    source_urls.append(job_location_map[callback["id"]])

    # Return the URLs of the processed data results
    return source_urls

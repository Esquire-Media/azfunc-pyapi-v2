# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/steps/addresses/device_ids.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
import uuid

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_addresses2deviceids(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the conversion of addresses to device IDs for Esquire audiences.

    This orchestrator processes addresses to generate corresponding device IDs, and optionally performs additional processing if custom coding is specified.

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
        "destination": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "source_urls": [str],
        "custom_coding": {
            "filter": bool
        }
    }
    """
    ingress = context.get_input()

    # Convert addresses to device IDs
    onspot = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "onspot_orchestrator",
                {
                    **ingress["working"],
                    "endpoint": "/save/addresses/all/devices",
                    "request": {
                        "hash": False,
                        "name": context.new_uuid(),
                        "fileName": context.new_uuid() + ".csv",
                        "fileFormat": {
                            "delimiter": ",",
                            "quoteEncapsulate": True,
                        },
                        "mappings": {
                            "street": ["address"],
                            "city": ["city"],
                            "state": ["state"],
                            "zip": ["zipCode"],
                            "zip4": ["plus4Code"],
                        },
                        "matchAcceptanceThreshold": 29.9,
                        "sources": [source_url.replace("https://", "az://")],
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

    if not ingress.get("custom_coding", {}).get("filter", False):
        source_urls = yield context.task_all([
            context.call_activity(
                "activity_esquireAudienceBuilder_formatDeviceIds",
                {
                    "source": url,
                    "destination": ingress["destination"]
                }
            )
            for url in source_urls
            if "debug.csv" not in url
        ])
        return source_urls

    # Further process the results if custom coding is specified
    demographics_results = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "onspot_orchestrator",
                {
                    **ingress["destination"],
                    "endpoint": "/save/files/demographics/all",
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
            for source_url in source_urls
        ]
    )

    # Collect URLs of the demographic results
    result_urls = []
    for result in demographics_results:
        job_location_map = {
            job["id"]: job["location"].replace("az://", "https://")
            for job in result["jobs"]
        }
        for callback in result["callbacks"]:
            if callback["success"]:
                if callback["id"] in job_location_map:
                    result_urls.append(job_location_map[callback["id"]])

    return result_urls

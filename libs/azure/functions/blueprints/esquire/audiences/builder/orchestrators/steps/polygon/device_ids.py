# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/steps/polygons/device_ids.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
from azure.storage.blob import BlobClient
import uuid, orjson as json

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_polygon2deviceids(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the conversion of polygons to device IDs for Esquire audiences.

    This orchestrator processes polygon data to generate corresponding device IDs, and optionally performs additional processing if custom coding is specified.

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
    destination = (
        ingress["working"]
        if ingress.get("custom_coding", {}).get("filter", False)
        else ingress["destination"]
    )

    # Format requests for converting polygons to device IDs
    requests = yield context.task_all(
        [
            context.call_activity(
                "activity_esquireAudienceBuilder_formatOnspotRequest",
                {
                    **ingress,
                    "source_url": source_url,
                },
            )
            for source_url in ingress["source_urls"]
        ]
    )

    # Process the requests to get device IDs
    device_ids_results = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "onspot_orchestrator",
                {
                    **destination,
                    "endpoint": "/save/geoframe/all/devices",
                    "request": json.loads(
                        BlobClient.from_blob_url(source_url).download_blob().readall()
                    ),
                },
            )
            for source_url in requests
        ]
    )

    # Collect URLs of the device IDs results
    source_urls = []
    for result in device_ids_results:
        job_location_map = {
            job["id"]: job["location"].replace("az://", "https://")
            for job in result["jobs"]
        }
        for callback in result["callbacks"]:
            if callback["success"]:
                if callback["id"] in job_location_map:
                    source_urls.append(job_location_map[callback["id"]])

    if not ingress.get("custom_coding", {}).get("filter", False):
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
                                    "name": uuid.uuid4().hex,
                                    "fileName": uuid.uuid4().hex + ".csv",
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

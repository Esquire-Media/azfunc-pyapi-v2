# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/processingSteps.py

from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions import Blueprint
import orjson as json

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_processingSteps(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the processing steps for Esquire audiences.

    This orchestrator processes the data through multiple steps, transforming it as specified by each process step. It handles various input and output data types, and can apply custom coding and filtering.

    Parameters:
    context (DurableOrchestrationContext): The context object provided by Azure Durable Functions, used to manage and track the orchestration.

    Returns:
    dict: The updated ingress data after all processing steps are completed.

    Expected format for context.get_input():
    {
        "source": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
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
            "dataSource": {
                "id": str,
                "dataType": str
            },
            "dataFilter": str,
            "processes": [
                {
                    "id": str,
                    "sort": str,
                    "outputType": str,
                    "customCoding": str
                }
            ]
        },
        "advertiser": {
            "id": str,
            "meta": str,
            "oneview": str,
            "xandr": str
        },
        "status": str,
        "rebuild": str,
        "rebuildUnit": str,
        "TTL_Length": str,
        "TTL_Unit": str,
        "results": [str]
    }
    """

    ingress = context.get_input()

    # Loop through each processing step
    for step, process in enumerate(
        processes := ingress["audience"].get("processes", [])
    ):
        # Determine the input type and source URLs for the current step
        inputType = (
            processes[step - 1]["outputType"]  # Use previous step's output type
            if step
            else ingress["audience"]["dataSource"]["dataType"]  # Use primary data type
        )
        source_urls = (
            processes[step - 1].get("results", []) if step else ingress["results"]
        )
        if not source_urls:
            raise Exception(
                "No data to process from previous step. [{}]: {} -> {}".format(
                    step - 1, inputType, process["outputType"]
                )
            )

        # Prepare custom coding for the first step or as specified
        if not step:
            custom_coding = {
                "request": {
                    "dateStart": {
                        "date_add": [
                            {"now": []},
                            0 - int(ingress["audience"]["TTL_Length"]),
                            ingress["audience"]["TTL_Unit"],
                        ]
                    },
                    "dateEnd": {"date_add": [{"now": []}, -2, "days"]},
                }
            }
        elif process.get("customCoding", False):
            try:
                custom_coding = json.loads(process["customCoding"])
            except:
                custom_coding = {}

        # Set up the egress data structure for the current step
        egress = {
            "working": {
                **ingress["working"],
                "blob_prefix": "{}/{}/{}/{}/working".format(
                    ingress["working"]["blob_prefix"],
                    ingress["instance_id"],
                    ingress["audience"]["id"],
                    step,
                ),
            },
            "destination": {
                **ingress["working"],
                "blob_prefix": "{}/{}/{}/{}".format(
                    ingress["working"]["blob_prefix"],
                    ingress["instance_id"],
                    ingress["audience"]["id"],
                    step,
                ),
            },
            "transform": [inputType, process["outputType"]],
            "source_urls": source_urls,
            "custom_coding": custom_coding,
        }

        # Process the data based on the input and output types
        match inputType:
            case "addresses":
                match process["outputType"]:
                    case "addresses":  # addresses -> addresses
                        # No specific processing required
                        pass
                    case "device_ids":  # addresses -> deviceids
                        process["results"] = yield context.call_sub_orchestrator(
                            "orchestrator_esquireAudiencesSteps_addresses2deviceids",
                            egress,
                        )
                    case "polygons":  # addresses -> polygons
                        process["results"] = yield context.task_all(
                            [
                                context.call_sub_orchestrator(
                                    "orchestrator_esquireAudienceMaidsAddresses_footprint",
                                    {
                                        **egress,
                                        "source": source_url,
                                    },
                                )
                                for source_url in source_urls
                            ]
                        )
            case "device_ids":
                match process["outputType"]:
                    case "addresses":  # deviceids -> addresses
                        process["results"] = yield context.call_sub_orchestrator(
                            "orchestrator_esquireAudiencesSteps_deviceids2addresses",
                            egress,
                        )
                    case "device_ids":  # deviceids -> deviceids
                        process["results"] = yield context.task_all(
                            [
                                context.call_sub_orchestrator(
                                    "orchestrator_esquireAudienceMaidsDeviceIds_todevids",
                                    {
                                        "working": egress["working"],
                                        "source": source_url,
                                        "destination": {
                                            **egress["destination"],
                                            "blob_name": "{}/results/{}.csv".format(
                                                egress["destination"]["blob_prefix"],
                                                index,
                                            ),
                                        },
                                    },
                                )
                                for index, source_url in enumerate(source_urls)
                            ]
                        )
                    case "polygons":  # deviceids -> polygons
                        # No specific processing required
                        pass
            case "polygons":
                match process["outputType"]:
                    case "addresses":  # polygons -> addresses
                        # No specific processing required
                        pass
                    case "device_ids":  # polygons -> deviceids
                        process["results"] = yield context.call_sub_orchestrator(
                            "orchestrator_esquireAudiencesSteps_polygon2deviceids",
                            egress,
                        )
                    case "polygons":  # polygons -> polygons
                        # No specific processing required
                        pass

        # Apply custom coding filters if specified
        if custom_coding.get("filter", False):
            process["results"] = yield context.task_all(
                [
                    context.call_activity(
                        "activity_esquireAudienceBuilder_filterResults",
                        {
                            "source": url,
                            "destination": egress["working"],
                            "filter": custom_coding["filter"],
                        },
                    )
                    for url in process["results"]
                ]
            )

    # Return the updated ingress data after all processing steps
    return ingress

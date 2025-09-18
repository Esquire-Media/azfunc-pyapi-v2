# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/processingSteps.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    extract_tenant_id_from_datafilter,
)
import orjson as json
import logging

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
            "processing": [
                {
                    "steps": [
                    {
                        "kind": str,
                        "{args}": str
                    }
                    ],
                    "version":int
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
        "rebuildSchedule": str,
        "TTL_Length": str,
        "TTL_Unit": str,
        "results": [str]
    }
    """
    logging.warning("[LOG] Processing Steps")

    ingress = context.get_input()

    # early retrn if we have no processing to do
    processing = ingress.get("audience", {}).get("processing")
    # if not processing or not processing.get("steps"):
    #     logging.warning("[LOG] No processing steps found, returning ingress unchanged.")
    #     return ingress

    ingress["base_prefix"]   = str(ingress["working"]["blob_prefix"]).strip("/")

    # Loop through each processing step
    for step, process in enumerate(
        processes := processing.get("steps",[])
    ):
        logging.warning(f"[LOG] Step: {step} - {process['kind']}")
        process["outputType"] = processing_step_output_types(process["kind"])
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

        # Set up the egress data structure for the current step

        egress = {
            "working": {
                **ingress["working"],
                "blob_prefix": "{}/{}/working".format(
                    ingress['base_prefix'],
                    step,
                ),
            },
            "destination": {
                **ingress["working"],
                "blob_prefix": "{}/{}".format(
                    ingress['base_prefix'],
                    step,
                ),
            },
            "transform": [inputType, process["outputType"]],
            "source_urls": source_urls,
            "process": process,
            "tenant_id": extract_tenant_id_from_datafilter(ingress["audience"]["dataFilter"])
        }

        # Process the data based on the input and output types
        # logging.warning(f"[LOG] Egress: {egress}")
        logging.warning(f"[LOG] Input Type: {inputType}")
        logging.warning(f"[LOG] Output Type: {process['outputType']}")

        match inputType:
            case "addresses":
                match process["outputType"]:
                    case "addresses":  # addresses -> addresses
                        logging.warning("[LOG] Addresses output type")
                        logging.warning("[LOG] Step Type:" + egress.get("process",{}).get("kind",""))
                        if egress["process"].get("kind", "") == "Neighbors":
                            # No specific processing required
                            process["results"] = yield context.call_sub_orchestrator(
                                "orchestrator_esquireAudiencesSteps_addresses2neighbors",
                                egress,
                            )
                        elif egress["process"].get("kind", "") == "Proximity":
                            process["results"] = yield context.call_sub_orchestrator(
                                "orchestrator_esquireAudiencesSteps_ownedLocationRadius",
                                egress
                            )
                        else:
                            logging.warning("[LOG] No Neighbor Logic used")
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

        logging.warning(f"[LOG] Step: {step} - {process['kind']} done.")

    # Ensure last step results are returned in ingress["results"]
    if processes:
        final_results = processes[-1].get("results", [])
        ingress["results"] = final_results
        ingress["audience"]["processing"][-1]["results"] = final_results

    # Return the updated ingress data after all processing steps
    return ingress

def processing_step_output_types(step_kind):
    return {
        "Proximity":"addresses",
        "Neighbors":"addresses"
    }[step_kind]
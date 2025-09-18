# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/finalize.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
import orjson as json
import logging
bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_finalize(
    context: DurableOrchestrationContext,
):
    """
    Finalizes the processing of Esquire audiences, ensuring the data is in the correct format and location.

    This orchestrator performs the final conversion to device IDs if necessary and stores the results in the specified destination.

    Parameters:
    context (DurableOrchestrationContext): The context object provided by Azure Durable Functions, used to manage and track the orchestration.

    Returns:
    dict: The updated ingress data after finalization.

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
        "results": [str]
    }
    """

    ingress = context.get_input()
    processing = ingress["audience"].get("processing", {})
    steps = processing.get("steps", [])
    has_steps = bool(steps)

    inputType = (
        steps[-1]["outputType"] if has_steps else ingress["audience"]["dataSource"]["dataType"]
    )
    source_urls = (
        steps[-1].get("results", []) if has_steps else ingress["results"]
    )

    # Check if there are source URLs to process
    if not source_urls:
        raise Exception(
            "No data to process from last step. [{}]: {}".format(steps, inputType)
        )

    # Reusable common input for sub-orchestrators
    egress = {
        "working": {
            **ingress["working"],
            "blob_prefix": "{}/{}".format(
                ingress["working"]["blob_prefix"],
                steps,
            ),
        },
        "destination": {
            **ingress["destination"],
            "blob_prefix": "{}/{}/{}".format(
                ingress["destination"]["blob_prefix"],
                ingress["audience"]["id"],
                context.current_utc_datetime.isoformat(),
            ),
        },
    }

    # Perform final conversion to device IDs if necessary
    match inputType:
        case "addresses":  # addresses -> deviceids
            source_urls = yield context.call_sub_orchestrator(
                "orchestrator_esquireAudiencesSteps_addresses2deviceids",
                {**egress, "source_urls": source_urls},
            )
        case "polygons":  # polygons -> deviceids
            source_urls = yield context.call_sub_orchestrator(
                "orchestrator_esquireAudiencesSteps_polygon2deviceids",
                {**egress, "source_urls": source_urls},
            )

    logging.info("[LOG] Did final conversion to deviceids")
    # Finalize and store the results
    ingress["results"] = yield context.task_all(
        [
            context.call_activity(
                "activity_esquireAudienceBuilder_finalize",
                {"source": source_url, "destination": egress["destination"]},
            )
            for source_url in source_urls
        ]
    )
    
    logging.info("[LOG] Getting MAID count")
    # Count results
    counts = yield context.task_all([
        context.call_activity(
            "activity_esquireAudiencesUtils_getMaidCount",
            source_url
        )
        for source_url in ingress["results"]
    ])
    logging.info("[LOG] Putting audience")
    ingress["audience"]["count"] = sum(counts)
    yield context.call_activity(
        "activity_esquireAudiencesBuilder_putAudience",
        ingress
    )
    logging.info("[LOG] Done finalizing.")
    # Return the updated ingress data
    return ingress

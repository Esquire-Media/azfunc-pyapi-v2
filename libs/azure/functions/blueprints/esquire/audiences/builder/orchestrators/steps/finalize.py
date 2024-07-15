# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/finalize.py

from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions import Blueprint
import orjson as json

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
            "processes": [
                {
                    "id": str,
                    "sort": str,
                    "outputType": str,
                    "customCoding": str
                }
            ],
            "TTL_Length": str,
            "TTL_Unit": str
        },
        "results": [str]
    }
    """

    ingress = context.get_input()
    steps = len(ingress["audience"].get("processes", []))
    inputType = (
        ingress["audience"]["processes"][-1]["outputType"]
        if steps
        else ingress["audience"]["dataSource"]["dataType"]
    )
    source_urls = (
        ingress["audience"]["processes"][-1]["results"] if steps else ingress["results"]
    )

    # Prepare custom coding for the first step or as specified
    if not steps:
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
    elif ingress["audience"]["processes"][-1].get("customCoding", False):
        try:
            custom_coding = json.loads(
                ingress["audience"]["processes"][-1]["customCoding"]
            )
        except:
            custom_coding = {}

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
        "custom_coding": custom_coding,
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
    
    # Count results
    counts = yield context.task_all([
        context.call_activity(
            "activity_esquireAudiencesUtils_getMaidCount",
            source_url
        )
        for source_url in ingress["results"]
    ])
    ingress["audience"]["count"] = sum(counts)
    yield context.call_activity(
        "activity_esquireAudiencesBuilder_putAudience",
        ingress
    )

    # Return the updated ingress data
    return ingress

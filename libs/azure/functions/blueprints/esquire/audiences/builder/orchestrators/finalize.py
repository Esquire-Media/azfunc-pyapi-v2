# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/finalize.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint

try:
    import orjson as json
except:
    import json

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_finalize(
    context: DurableOrchestrationContext,
):
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

    if not source_urls:
        raise Exception(
            "No data to process from last step. [{}]: {}".format(steps, inputType)
        )

    # Reusable common input for sub-orchestrators
    egress = {
        "working": {
            **ingress["working"],
            "blob_prefix": "{}/{}/{}/{}".format(
                ingress["working"]["blob_prefix"],
                ingress["instance_id"],
                ingress["audience"]["id"],
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

    # Do a final conversion to device IDs here if necessary
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

    yield context.task_all(
        [
            context.call_activity(
                "activity_esquireAudienceBuilder_finalize",
                {"source": source_url, "destination": egress["destination"]},
            )
            for source_url in source_urls
        ]
    )

    return ingress

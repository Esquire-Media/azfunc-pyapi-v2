# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/finalize.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint

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
        ingress["audience"]["processes"][-1]["results"]
        if steps
        else ingress["results"]
    )

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

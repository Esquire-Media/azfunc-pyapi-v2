# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/finalize.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_finalize(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()

    # Reusable common input for sub-orchestrators
    egress = {
        "working": {
            **ingress["working"],
            "blob_prefix": "{}/{}/{}/{}/working".format(
                ingress["working"]["blob_prefix"],
                ingress["instance_id"],
                ingress["audience"]["id"],
                len(ingress["processes"]),
            ),
        }
    }
    egress["destination"] = {
        **egress["working"],
        "blob_name": "{}/results.csv".format(ingress["working"]["blob_prefix"]),
    }

    # Do a final conversion to device IDs here if necessary
    match (
        ingress["processes"][-1]["outputType"]
        if len(ingress["processes"])
        else ingress["dataSource"]["dataType"]
    ):
        case "addresses":  # addresses -> deviceids
            ingress["results"] = yield context.task_all(
                [
                    context.call_sub_orchestrator(
                        "orchestrator_esquireAudienceMaidsAddresses_standard",
                        {
                            **egress,
                            "source": source_url,
                        },
                    )
                    for source_url in (
                        ingress["processes"][-1]["results"]
                        if len(ingress["processes"])
                        else ingress["results"]
                    )
                ]
            )
        case "deviceids":  # deviceids -> deviceids
            # No tranformation, just set the results using the last process
            ingress["results"] = (
                ingress["processes"][-1]["results"]
                if len(ingress["processes"])
                else ingress["results"]
            )
        case "polygons":  # polygons -> deviceids
            ingress["results"] = yield context.task_all(
                [
                    context.call_sub_orchestrator(
                        "orchestrator_esquireAudienceMaidsGeoframes_standard",
                        {
                            **egress,
                            "source": source_url,
                        },
                    )
                    for source_url in (
                        ingress["processes"][-1]["results"]
                        if len(ingress["processes"])
                        else ingress["results"]
                    )
                ]
            )

    return ingress

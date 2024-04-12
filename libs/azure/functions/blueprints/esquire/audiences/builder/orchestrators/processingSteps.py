# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/processingSteps.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_processingSteps(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()

    # Loop through processing steps
    for step, process in enumerate(
        processes := ingress["audience"].get("processes"), []
    ):
        # Reusable common input for sub-orchestrators
        egress = {
            "working": {
                **ingress["working"],
                "blob_prefix": "{}/{}/{}/{}/working".format(
                    ingress["working"]["blob_prefix"],
                    ingress["instance_id"],
                    ingress["audience"]["id"],
                    step,
                ),
            }
        }
        egress["destination"] = {
            **egress["working"],
            "blob_name": "{}/results.csv".format(ingress["working"]["blob_prefix"]),
        }

        # Switch logistics based on the input data type
        match (
            processes[step - 1]["outputType"]  # Use previous step's output type
            if step
            else ingress["audience"]["dataSource"]["dataType"]  # Use primary data type
        ):
            case "addresses":
                match process["outputType"]:
                    case "addresses":  # addresses -> addresses
                        # TODO: figure out what (if anything) should happen here
                        pass
                    case "deviceids":  # addresses -> deviceids
                        process["results"] = yield context.task_all(
                            [
                                context.call_sub_orchestrator(
                                    "orchestrator_esquireAudienceMaidsAddresses_standard",
                                    {
                                        **egress,
                                        "source": source_url,
                                    },
                                )
                                for source_url in (
                                    processes[step - 1]["results"]
                                    if step
                                    else ingress["results"]
                                )
                            ]
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
                                for source_url in (
                                    processes[step - 1]["results"]
                                    if step
                                    else ingress["results"]
                                )
                            ]
                        )
            case "deviceids":
                match process["outputType"]:
                    case "addresses":  # deviceids -> addresses
                        # onspot /save/files/household
                        pass
                    case "deviceids":  # deviceids -> deviceids
                        # onspot /save/files/demographics/all
                        pass
                    case "polygons":  # deviceids -> polygons
                        # TODO: figure out what (if anything) should happen here
                        pass
            case "polygons":
                match process["outputType"]:
                    case "addresses":  # polygons -> addresses
                        # onspot /save/files/household
                        pass
                    case "deviceids":  # polygons -> deviceids
                        process["results"] = yield context.task_all(
                            [
                                context.call_sub_orchestrator(
                                    "orchestrator_esquireAudienceMaidsGeoframes_standard",
                                    {
                                        **egress,
                                        "source": source_url,
                                    },
                                )
                                for source_url in (
                                    processes[step - 1]["results"]
                                    if step
                                    else ingress["results"]
                                )
                            ]
                        )
                    case "polygons":  # polygons -> polygons
                        # TODO: figure out what (if anything) should happen here
                        pass

    return ingress

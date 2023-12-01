# File: libs/azure/functions/blueprints/esquire/audiences/maids/orchestrators/addresses.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from libs.azure.functions import Blueprint

bp = Blueprint()


# main orchestrator
@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesMaids_addresses(context: DurableOrchestrationContext):
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    unvalidated_addresses_name = "addresses.csv"

    yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_esquireAudiencesMaids_newMovers",
                retry,
                {
                    "audience": audience,
                    "destination": {
                        **ingress["destination"],
                        "blob_name": "{}/{}/{}".format(
                            ingress["destination"]["blob_prefix"],
                            audience["id"],
                            unvalidated_addresses_name,
                        ),
                    },
                },
            )
            for audience in ingress["audiences"]
            if audience["type"] in ["New Movers"]
        ]
    )

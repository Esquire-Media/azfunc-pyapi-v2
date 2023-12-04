# File: libs/azure/functions/blueprints/esquire/audiences/maids/orchestrators/geoframes.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.maids.config import geoframes_name

bp = Blueprint()


# main orchestrator
@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesMaids_geoframes(context: DurableOrchestrationContext):
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_esquireAudiencesMaidsGeoframes_geoframes",
                retry,
                {
                    "audience": audience,
                    "destination": {
                        **ingress["destination"],
                        "blob_name": "{}/{}/{}".format(
                            ingress["destination"]["blob_prefix"],
                            audience["id"],
                            geoframes_name,
                        ),
                    },
                },
            )
            for audience in ingress["audiences"]
            if audience["type"] in ["InMarket Shoppers", "Competitor Locations"]
        ]
    )

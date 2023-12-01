# File: libs/azure/functions/blueprints/esquire/audiences/maids/geoframes/orchestrators/standard.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint

bp = Blueprint()


# main orchestrator for friends and family audiences (suborchestrator for the root)
## one audience at a time
@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudienceMaidsGeoframes_standard(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    # pass Friends and Family to OnSpot Orchestrator
    onspot = yield context.task_all(
        [
            context.call_sub_orchestrator_with_retry(
                "onspot_orchestrator",
                retry,
                {
                    **ingress["working"],
                    "endpoint": "/save/geoframe/all/devices",
                    "request": BlobClient.from_blob_url(ingress["source"])
                    .download_blob()
                    .readall(),
                },
            )
        ]
    )

    # check if all callbacks succeeded
    if not all([c["success"] for r in onspot for c in r["callbacks"]]):
        # if there are failures, throw exception of what failed in the call
        # TODO: exception for submission failures
        raise Exception([c for r in onspot for c in r["callbacks"] if not c["success"]])

    # merge all of the device files into one file
    yield context.call_activity_with_retry(
        "activity_onSpot_mergeDevices",
        retry,
        {
            "source": ingress["working"],
            "destination": ingress["destination"],
        },
    )

    return {}

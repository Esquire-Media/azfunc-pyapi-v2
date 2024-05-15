# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/transformations/geoframeToDeviceIds.py

from azure.durable_functions import DurableOrchestrationContext
from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint

try:
    import orjson as json
except:
    import json

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesProcessingSteps_geoframeToDeviceIds(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()

    requests = yield context.task_all(
        [
            context.call_activity(
                "activity_esquireAudienceBuilder_formatOnspotRequest",
                {
                    **ingress,
                    "source_url": source_url,
                },
            )
            for source_url in ingress["source_urls"]
        ]
    )
    onspot = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "onspot_orchestrator",
                {
                    **ingress["working"],
                    "endpoint": "/save/geoframe/all/devices",
                    "request": json.loads(
                        BlobClient.from_blob_url(source_url).download_blob().readall()
                    ),
                },
            )
            for source_url in requests
        ]
    )
    results = []
    for result in onspot:
        job_location_map = {
            job["id"]: job["location"].replace("az://", "https://")
            for job in result["jobs"]
        }
        for callback in result["callbacks"]:
            if callback["success"]:
                if callback["id"] in job_location_map:
                    results.append(job_location_map[callback["id"]])

    return results

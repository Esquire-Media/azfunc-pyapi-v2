from azure.durable_functions import Blueprint, DurableOrchestrationContext
from libs.utils.azure_storage import download_blob_bytes
import orjson as json

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_polygon2deviceidcounts(
    context: DurableOrchestrationContext,
):
    """
    Same structure as orchestrator_esquireAudiencesSteps_polygon2deviceids but uses
    /save/geoframe/all/countgroupedbydevice and returns count outputs.
    """

    ingress = context.get_input()
    destination = (
        ingress["working"]
        if ingress.get("custom_coding", {}).get("filter", False)
        else ingress["destination"]
    )

    # 1) Build OnSpot request blobs (deterministic fan-out)
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

    # 2) Submit each request to OnSpot (countgroupedbydevice)
    count_results = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "onspot_orchestrator",
                {
                    **destination,
                    "endpoint": "/save/geoframe/all/countgroupedbydevice",
                    "request": json.loads(
                        download_blob_bytes(source_url)
                    ),
                },
            )
            for source_url in requests
        ]
    )

    # 3) Extract successful job locations
    source_urls = []
    for result in count_results:
        job_location_map = {
            job["id"]: job["location"].replace("az://", "https://")
            for job in result["jobs"]
        }
        for callback in result["callbacks"]:
            if callback["success"] and callback["id"] in job_location_map:
                source_urls.append(job_location_map[callback["id"]])

    return source_urls

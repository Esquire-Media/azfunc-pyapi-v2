# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/steps/polygons/device_ids.py

from azure.durable_functions import DurableOrchestrationContext
from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
import uuid

try:
    import orjson as json
except:
    import json

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_polygon2deviceids(
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
    device_ids_results = yield context.task_all(
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
    source_urls = []
    for result in device_ids_results:
        job_location_map = {
            job["id"]: job["location"].replace("az://", "https://")
            for job in result["jobs"]
        }
        for callback in result["callbacks"]:
            if callback["success"]:
                if callback["id"] in job_location_map:
                    source_urls.append(job_location_map[callback["id"]])
                    
    if not ingress["custom_coding"]:
        return source_urls
    
    demographics_results = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "onspot_orchestrator",
                {
                    **ingress["working"],
                    "endpoint": "/save/files/demographics/all",
                    "request": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Files",
                                "paths": [source_url.replace("https://", "az://")],
                                "properties": {
                                    "name": uuid.uuid4().hex,
                                    "fileName": uuid.uuid4().hex + ".csv",
                                    "hash": False,
                                    "fileFormat": {
                                        "delimiter": ",",
                                        "quoteEncapsulate": True,
                                    },
                                },
                            }
                        ],
                    },
                },
            )
            for source_url in source_urls
        ]
    )
    result_urls = []
    for result in demographics_results:
        job_location_map = {
            job["id"]: job["location"].replace("az://", "https://")
            for job in result["jobs"]
        }
        for callback in result["callbacks"]:
            if callback["success"]:
                if callback["id"] in job_location_map:
                    result_urls.append(job_location_map[callback["id"]])

    return result_urls

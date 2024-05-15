# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/steps/addresses/device_ids.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
import uuid

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_addresses2deviceids(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    onspot = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "onspot_orchestrator",
                {
                    **ingress["working"],
                    "endpoint": "/save/addresses/all/devices",
                    "request": {
                        "hash": False,
                        "name": uuid.uuid4().hex,
                        "fileName": uuid.uuid4().hex + ".csv",
                        "fileFormat": {
                            "delimiter": ",",
                            "quoteEncapsulate": True,
                        },
                        "mappings": {
                            "street": ["address"],
                            "city": ["city"],
                            "state": ["state"],
                            "zip": ["zipCode"],
                            "zip4": ["plus4Code"],
                        },
                        "matchAcceptanceThreshold": 29.9,
                        "sources": [source_url.replace("https://", "az://")],
                    },
                },
            )
            for source_url in ingress["source_urls"]
        ]
    )
    source_urls = []
    for result in onspot:
        job_location_map = {
            job["id"]: job["location"].replace("az://", "https://")
            for job in result["jobs"]
        }
        for callback in result["callbacks"]:
            if callback["success"]:
                if callback["id"] in job_location_map:
                    source_urls["results"].append(job_location_map[callback["id"]])
                    
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

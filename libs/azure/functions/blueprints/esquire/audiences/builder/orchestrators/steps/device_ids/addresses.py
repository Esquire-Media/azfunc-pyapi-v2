# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/steps/device_ids/addresses.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
import uuid, logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_deviceids2addresses(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    onspot = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "onspot_orchestrator",
                {
                    **ingress["working"],
                    "endpoint": "/save/files/household",
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
                    source_urls.append(job_location_map[callback["id"]])

    # result_urls = yield context.task_all(
    #     [
    #         context.call_activity(
    #             "activity_esquireAudienceBuilder_addressCompletion",
    #             {"source": source_url, "destination": ingress["working"]},
    #         )
    #         for source_url in source_urls
    #     ]
    # )

    return source_urls

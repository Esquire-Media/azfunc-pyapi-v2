
from azure.durable_functions import Blueprint, DurableOrchestrationContext
import uuid

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_deviceids2Demographics(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the conversion of device IDs to demographics for Esquire audiences.

    This orchestrator processes device IDs to generate corresponding demographics and returns the URLs of the processed data.

    Parameters:
    context (DurableOrchestrationContext): The context object provided by Azure Durable Functions, used to manage and track the orchestration.

    Returns:
    list: The URLs of the processed data results.

    Expected format for context.get_input():
    {
        "working": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "source_urls": [str]
    }
    """

    ingress = context.get_input()

    demographics_results = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "onspot_orchestrator",
                {
                    **ingress["destination"],
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
            for source_url in ingress["source_urls"]
        ]
    )

    # Collect URLs of the demographic results
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
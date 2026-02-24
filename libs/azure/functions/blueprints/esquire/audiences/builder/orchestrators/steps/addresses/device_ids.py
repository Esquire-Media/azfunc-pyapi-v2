from azure.durable_functions import Blueprint, DurableOrchestrationContext

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_addresses2deviceids(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the conversion of addresses to device IDs for Esquire audiences.

    This orchestrator processes addresses to generate corresponding device IDs, and optionally performs additional processing if custom coding is specified.

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
        "destination": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "source_urls": [str],
        "custom_coding": {
            "filter": bool
        }
    }
    """
    ingress = context.get_input()

    # Merge all source URLs into a single blob first
    merged_url = yield context.call_activity(
        "activity_esquireAudienceBuilder_mergeSources",
        {
            "source_urls": ingress["source_urls"],
            "destination": ingress["working"],
        },
    )

    # Single onspot orchestrator call with combined URL
    result = yield context.call_sub_orchestrator(
        "onspot_orchestrator",
        {
            **ingress["working"],
            "endpoint": "/save/addresses/all/devices",
            "request": {
                "hash": False,
                "name": context.new_uuid(),
                "fileName": context.new_uuid() + ".csv",
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
                "sources": [merged_url.replace("https://", "az://")],
            },
        },
    )

    # Collect URL of the converted result
    job_location_map = {
        job["id"]: job["location"].replace("az://", "https://")
        for job in result["jobs"]
    }
    source_url = None
    for callback in result["callbacks"]:
        if callback["success"]:
            if callback["id"] in job_location_map:
                source_url = job_location_map[callback["id"]]
                break

    # Format the device IDs for the single source
    formatted_url = yield context.call_activity(
        "activity_esquireAudienceBuilder_formatDeviceIds",
        {
            "source": source_url,
            "destination": ingress["destination"]
        }
    )
    return [formatted_url]

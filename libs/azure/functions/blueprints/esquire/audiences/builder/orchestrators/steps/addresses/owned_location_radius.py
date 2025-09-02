from azure.durable_functions import Blueprint, DurableOrchestrationContext
import pandas as pd
import logging
bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_ownedLocationRadius(
    context: DurableOrchestrationContext,
):
    logging.info("[LOG] Starting the ownedLocationRadius functions")
    ingress = context.get_input()

    # first we get the owned locations for that tenant
    ingress["owned_locations"] = context.call_activity(
        "activity_esquireAudiencesNeighbors_findTenantOwnedLocations",
        ingress
    )

    # Step 2: Fan out radius filtering for each sales blob
    filter_tasks = [
        context.call_activity(
            "activity_esquireAudiencesNeighbors_addressRadialLimitation",
            {
                "sales_blob_url": url,
                "owned_locations": ingress["owned_locations"],  # list of {"latitude", "longitude"}
                "radius_miles": ingress.get('radius_miles', 10)
            }
        )
        for url in ingress["source_urls"]
    ]
    results = yield context.task_all(filter_tasks)

    # Step 3: Fan out writing blobs
    write_tasks = [
        context.call_activity(
            "activity_write_blob",
            {
                "records": recs,
                "container": ingress["destination"]["container_name"],
                "blob_prefix": ingress["destination"]["blob_prefix"],
                "conn_setting": "AzureWebJobsStorage",
                "preflight": True,
            }
        )
        for recs in results
    ]

    out_urls = yield context.task_all(write_tasks)
    return out_urls
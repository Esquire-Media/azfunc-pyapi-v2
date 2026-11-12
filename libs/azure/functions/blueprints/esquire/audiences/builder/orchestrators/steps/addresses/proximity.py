from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_ownedLocationRadius(
    context: DurableOrchestrationContext,
):
    # logging.warning("[LOG] Starting the ownedLocationRadius functions")
    ingress = context.get_input()

    # first we get the owned locations for that tenant
    ingress["owned_locations"] = yield context.call_activity(
        "activity_esquireAudiencesNeighbors_findTenantOwnedLocations",
        ingress
    )
    if not ingress["owned_locations"] or not len(ingress["owned_locations"]):
        raise Exception(
            "No market owned locations found; unable to complete Proximity step."
        )

    # Coordinate enrichment is separate from AddrAll/autocomplete and needed
    # only for spatial audiences. Bound DB-heavy geocode activity fan-out.
    working = ingress.get("working") or ingress["destination"]
    coordinate_urls = []
    source_urls = list(ingress["source_urls"])
    retry = RetryOptions(first_retry_interval_in_milliseconds=5000, max_number_of_attempts=3)
    for start in range(0, len(source_urls), 3):
        tasks = [
            context.call_activity_with_retry(
                "activity_esquireAudiences_geocodeMissingCoordinates",
                retry,
                {
                    "source_url": url,
                    "output_name": f"source-{source_index:05d}.csv",
                    "destination": {
                        "conn_str": working.get("conn_str", "AzureWebJobsStorage"),
                        "container_name": working["container_name"],
                        "blob_prefix": f"{working['blob_prefix']}/latlon/radius",
                    },
                },
            )
            for source_index, url in enumerate(
                source_urls[start:start + 3], start=start
            )
        ]
        coordinate_urls.extend(result["url"] for result in (yield context.task_all(tasks)))

    # Step 2: Fan out radius filtering for each enriched sales blob
    filter_tasks = [
        context.call_activity(
            "activity_esquireAudiencesNeighbors_addressRadialLimitation",
            {
                "sales_blob_url": url,
                "owned_locations": ingress["owned_locations"],  # list of {"latitude", "longitude"}
                "radius_miles": ingress.get('radius_miles', 10)
            }
        )
        for url in coordinate_urls
    ]
    results = yield context.task_all(filter_tasks)
    if not results or not len(results):
        raise Exception(
            f"No addresses found within {ingress.get('radius_miles', 10)} miles of market owned locations; unable to complete Proximity step."
        )

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
# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/steps/addresses/device_ids.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
import pandas as pd
import logging
bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_addresses2neighbors(
    context: DurableOrchestrationContext,
):
    logging.info("[LOG] Starting the addresses2neighbors functions")
    ingress = context.get_input()

    logging.info("[LOG] Getting partitions")
    # Step 1: Get partition key tuples only — low memory
    partition_keys = yield context.call_activity("activity_esquireAudiencesNeighbors_extractPartitions", 
        ingress
        )

    # Step 2: Query 
    logging.info(f"[LOG] Setting {len(partition_keys)} tasks")
    tasks = []
    for part in partition_keys:
        # logging.info(f"[LOG] Partition {part}")
        tasks.append(context.call_activity("activity_esquireAudiencesNeighbors_findNeighbors", {
            "city": part["city"],
            "state": part["state"],
            "zip": part["zip"],
            "n_per_side": ingress.get('customCoding',{}).get("neighbors_query",{}).get("n_per_side", 20),
            "same_side_only": ingress.get('customCoding',{}).get("neighbors_query",{}).get("same_side_only", False),
            "limit": ingress.get('customCoding',{}).get("neighbors_query",{}).get("limit", -1),
            "source_urls": ingress.get('source_urls',[]),
        }))

    results = yield context.task_all(tasks)

    # Step 3: Merge + dedupe
    # logging.info(f"[LOG] Merging output and deduping from {len(results)} result groups")
    combined = [rec for r in results if r for rec in r]  # flatten list[list[dict]]
    seen, deduped = set(), []
    for rec in combined:
        key = (rec.get("address"), rec.get("city"), rec.get("zipCode"))
        if key not in seen:
            seen.add(key)
            deduped.append(rec)

    # Step 4: Write output
    # blob_id = uuid.uuid4().hex
    # logging.info(f"[LOG] Writing output to {ingress['destination']['blob_prefix']}/blob_id")
    # blob_name = f"{ingress['destination']['blob_prefix']}/{blob_id}"
    # out_url = yield context.call_activity("activity_esquireAudienceBuilder_writeBlob", 
    #     {
    #         "records": deduped,
    #         "container": ingress["destination"]["container_name"],
    #         "blobName": blob_name
    #     }
    # )
    out_url = yield context.call_activity(
    "activity_write_blob",
        {
            "records": deduped,
            "container": ingress["destination"]["container_name"],
            "blob_prefix": f"{ingress['destination']['blob_prefix']}",   # or whatever your layout is
            "conn_str": "AzureWebJobsStorage",
            "preflight": True,  # optional
        },
    )

    logging.info(f"Output url: {out_url}")

    return [out_url]
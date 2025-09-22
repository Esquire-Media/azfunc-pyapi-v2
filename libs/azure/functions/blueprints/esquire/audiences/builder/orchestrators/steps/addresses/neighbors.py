# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/steps/addresses/device_ids.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
import pandas as pd
# import logging
bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_addresses2neighbors(
    context: DurableOrchestrationContext,
):
    # logging.warning("[LOG] Starting the addresses2neighbors functions")
    ingress = context.get_input()

    # logging.warning("[LOG] Getting partitions")
    # Step 1: Get partition key tuples only — low memory
    partition_keys = yield context.call_activity("activity_esquireAudiencesNeighbors_extractPartitions", 
        ingress
        )

    # Step 2: Query 
    # logging.warning(f"[LOG] Setting {len(partition_keys)} tasks")
    tasks = []
    for part in partition_keys:
        # logging.warning(f"[LOG] Partition {part}")
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

    # Step 3: fan-out to write each records batch to blob
    write_tasks = [
        context.call_activity(
            "activity_write_blob",
            {
                "records": recs,
                "container": ingress["destination"]["container_name"],
                "blob_prefix": f"{ingress['destination']['blob_prefix']}",
                "conn_setting": "AzureWebJobsStorage",  # name of setting, not the raw conn string
                "preflight": True,
            },
        )
        for recs in results
    ]

    out_urls = yield context.task_all(write_tasks) 

    return out_urls

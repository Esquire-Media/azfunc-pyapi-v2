# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/steps/addresses/device_ids.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_addresses2neighbors(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()

    # Step 1: Get partition key tuples only — low memory
    partition_keys = yield context.call_activity("activity_esquireBlob_extractPartitions", {
        "url": ingress["inputBlob"]
    })

    # Step 2: Query 
    tasks = []
    for part in partition_keys:
        tasks.append(context.call_activity("activity_esquireAudiencesBuilder_findNeighbors", {
            "city": part["city"],
            "state": part["state"],
            "zip": part["zip"],
            "n_per_side": ingress.get('custom_coding',{}).get("n_per_side", 100),
            "same_side_only": ingress.get('custom_coding',{}).get("same_side_only", True),
            "limit": ingress.get('custom_coding',{}).get("limit", -1)
        }))

    results = yield context.task_all(tasks)

    # Step 3: Merge + dedupe
    combined = [a for group in results for a in group]
    seen = set()
    deduped = []
    for a in combined:
        key = (a.get("address"), a.get("city"), a.get("zipCode"))
        if key not in seen:
            deduped.append(a)
            seen.add(key)

    # Step 4: Write output
    blob_name = f"{ingress['destination']['blob_prefix']}/results/neighbors.csv"
    out_url = yield context.call_activity("activity_esquireBlob_writeCsv", {
        "records": deduped,
        "container": ingress["destination"]["container_name"],
        "blobName": blob_name
    })

    return [out_url]
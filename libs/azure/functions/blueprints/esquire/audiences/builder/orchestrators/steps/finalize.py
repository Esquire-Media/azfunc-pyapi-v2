from azure.durable_functions import Blueprint, DurableOrchestrationContext
from typing import Any, Dict, List
import os
import logging

bp = Blueprint()

# Deterministic default (can be overridden via ingress for tuning).
_DEFAULT_MAX_PARALLEL = 50
MAX_FINAL_BLOBS = int(os.getenv("FINALIZE_MAX_BLOBS", "200"))

def _chunk(items: List[Any], size: int) -> List[List[Any]]:
    if size <= 0:
        size = _DEFAULT_MAX_PARALLEL
    return [items[i : i + size] for i in range(0, len(items), size)]

@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_finalize(
    context: DurableOrchestrationContext,
):
    """
    Finalizes the processing of Esquire audiences, ensuring the data is in the correct format and location.

    This orchestrator performs the final conversion to device IDs if necessary and stores the results in the specified destination.

    Parameters:
    context (DurableOrchestrationContext): The context object provided by Azure Durable Functions, used to manage and track the orchestration.

    Returns:
    dict: The updated ingress data after finalization.

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
        "audience": {
            "id": str,
            "dataSource": {
                "id": str,
                "dataType": str
            },
            "dataFilter": str,
            "processing": [
                {
                    "steps": [
                    {
                        "kind": str,
                        "{args}": str
                    }
                    ],
                    "version":int
                }
            ]
        },
        "results": [str]
    }
    """

    ingress = context.get_input()
    processing = ingress["audience"].get("processing", {})
    steps = processing.get("steps", []) if processing else []
    has_steps = bool(steps)
    logging.warning(f"[LOG] FINALIZE INFO")
    # logging.warning(f"[LOG] ingress: {ingress}")
    logging.warning(f"[LOG] processing: {processing}")
    logging.warning(f"[LOG] steps: {steps}")
    logging.warning(f"[LOG] has_steps: {has_steps}")

    inputType = (
        steps[-1]["outputType"]
        if has_steps
        else ingress["audience"]["dataSource"]["dataType"]
    )
    source_urls = steps[-1].get("results", []) if has_steps else ingress["results"]

    # logging.warning(f"[LOG] source_urls: {source_urls}")

    # Check if there are source URLs to process
    if not source_urls:
        raise Exception(
            "No data to process from last step. [{}]: {}".format(steps, inputType)
        )

    # Reusable common input for sub-orchestrators
    egress = {
        "working": {
            **ingress["working"],
            "blob_prefix": "{}/{}".format(
                ingress["working"]["blob_prefix"],
                len(steps),
            ),
        },
        "destination": {
            **ingress["destination"],
            "blob_prefix": "{}/{}/{}".format(
                ingress["destination"]["blob_prefix"],
                ingress["audience"]["id"],
                context.current_utc_datetime.isoformat(),
            ),
        },
    }

    # Perform final conversion to device IDs if necessary
    match inputType:
        case "addresses":  # addresses -> deviceids
            source_urls = yield context.call_sub_orchestrator(
                "orchestrator_esquireAudiencesSteps_addresses2deviceids",
                {**egress, "source_urls": source_urls},
            )
        case "polygons":  # polygons -> deviceids
            source_urls = yield context.call_sub_orchestrator(
                "orchestrator_esquireAudiencesSteps_polygon2deviceids",
                {
                    **egress,
                    "source_urls": source_urls,
                    "custom_coding": {
                        "request": {
                            "dateStart": {
                                "date_add": [
                                    "now",
                                    0 - ingress["audience"]["TTL_Length"],
                                    ingress["audience"]["TTL_Unit"],
                                ]
                            },
                            "dateEnd": {"date_add": ["now", -2, "days"]},
                        },
                    },
                },
            )

    # batch them if we need to
    N = len(source_urls)
    if N <= MAX_FINAL_BLOBS:
        finalized_urls = yield context.task_all(
            [
                context.call_activity(
                    "activity_esquireAudienceBuilder_finalize",
                    {
                        "batch_index": i,
                        "source": [source_url],
                        "destination": egress["destination"],
                    },
                )
                for i, source_url in enumerate(source_urls)
            ]
        )
    else:
        # batch them together into a maximum of 200 blobs
        num_outputs = MAX_FINAL_BLOBS
        per_output = (N + num_outputs - 1) // num_outputs  # ceil division

        finalized_urls = []
        for i in range(num_outputs):
            start = i * per_output
            end = min(start + per_output, N)
            batch = source_urls[start:end]

            if not batch:
                break

            result = yield context.call_activity(
                "activity_esquireAudienceBuilder_finalize",
                {
                    "batch_index": i,
                    "source": batch,
                    "destination": egress["destination"],
                },
            )
            finalized_urls.append(result)
    
    ingress["results"] = finalized_urls
    logging.warning("[LOG] Got finalized urls.")

    logging.warning("[LOG] Getting maid counts.")
    # Fan-out MAID counts in bounded batches too
    counts: List[int] = []
    for batch in _chunk(list(ingress["results"]), _DEFAULT_MAX_PARALLEL):
        batch_counts = yield context.task_all(
            [
                context.call_activity("activity_esquireAudiencesUtils_getMaidCount", url)
                for url in batch
            ]
        )
        counts.extend(batch_counts)

    logging.warning("[LOG] Summing maid counts.")
    ingress["audience"]["count"] = sum(counts)

    logging.warning("[LOG] Putting audience.")
    yield context.call_activity("activity_esquireAudiencesBuilder_putAudience", ingress)
    logging.warning("[LOG] Done finalizing.")
    # logging.info("[LOG] Done finalizing.")
    # Return the updated ingress data
    return ingress

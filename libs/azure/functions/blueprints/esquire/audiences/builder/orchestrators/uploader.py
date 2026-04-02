# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/uploader.py

import json
import os

from azure.durable_functions import Blueprint, DurableOrchestrationContext

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_noop(context: DurableOrchestrationContext):
    """
    Deterministic no-op sub-orchestrator used to keep schedule shape stable.
    """
    _ = context.get_input()  # ignored intentionally
    return None


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_uploader(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the building of Esquire audiences.

    This orchestrator performs several steps to process audience data, including fetching audience details, generating primary datasets, running processing steps, finalizing data, and pushing the data to configured DSPs.

    Parameters:
    context (DurableOrchestrationContext): The context object provided by Azure Durable Functions, used to manage and track the orchestration.

    Returns:
    dict: The final state of the ingress data after processing.

    Expected format for context.get_input():
    {
        "destination": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "audience": {
            "id": str,
            "advertiser": {
                "meta": str,
                "xandr": str,
            }
        },
    }
    """

    ingress = context.get_input()
    advertiser = (ingress.get("audience", {}) or {}).get("advertiser", {}) or {}
    audience_id = ingress.get("audience", {}).get("id")

    # Targets in deterministic, fixed order
    targets = [
        ("esquire-freewheel", bool(advertiser.get("freewheel"))),
        ("esquire-meta", bool(advertiser.get("meta"))),
    ]

    # Always schedule exactly 2 HTTP calls in that order.
    tasks = []
    for func_app_name, enabled in targets:
        if enabled:
            tasks.append(
                context.call_http(
                    method="POST",
                    uri=f"https://{func_app_name}.azurewebsites.net/api/{audience_id}",
                )
            )
        else:
            # Keep the schedule shape constant with a no-op
            tasks.append(
                context.call_sub_orchestrator(
                    "orchestrator_noop",
                    {"reason": f"{func_app_name} disabled", "audienceId": audience_id},
                )
            )

    # Wait for both to complete (order preserved)
    all_results = yield context.task_all(tasks)

    if any(type(res) == dict for res in all_results):
        return [res for res in all_results if type(res) == dict][0]
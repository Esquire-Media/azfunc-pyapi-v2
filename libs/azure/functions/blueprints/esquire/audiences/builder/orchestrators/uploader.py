# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/uploader.py

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

    # Targets in deterministic, fixed order
    targets = [
        ("freewheel_segment_orchestrator", bool(advertiser.get("freewheel"))),
        ("meta_customaudience_orchestrator", bool(advertiser.get("meta"))),
        # ("xandr_segment_orchestrator", bool(advertiser.get("xandr"))),
    ]

    # Always schedule exactly 2 sub-orchestrations in that order.
    tasks = []
    for orch_name, enabled in targets:
        if enabled:
            tasks.append(context.call_sub_orchestrator(orch_name, ingress))
        else:
            # Keep the schedule shape constant with a no-op
            tasks.append(
                context.call_sub_orchestrator(
                    "orchestrator_noop",
                    {"reason": f"{orch_name} disabled", "audienceId": ingress.get("audience", {}).get("id")},
                )
            )

    # Wait for both to complete (order preserved)
    all_results = yield context.task_all(tasks)

    if any(type(res) == dict for res in all_results):
        return [res for res in all_results if type(res) == dict][0]
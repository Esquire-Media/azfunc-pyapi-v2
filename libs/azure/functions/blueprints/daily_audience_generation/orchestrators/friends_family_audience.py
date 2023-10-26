# File: libs/azure/functions/blueprints/daily_audience_generation/orchestrators/friends_family_audience.py

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions

bp: Blueprint = Blueprint()


# main orchestrator for friends and family audiences (suborchestrator for the root)
@bp.orchestration_trigger(context_name="context")
def suborchestrator_friends_family(context: DurableOrchestrationContext):
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)
    # suborchestrator for the rooftop polys
    yield context.task_all(
        [
            # testing for friends and family with sample file
            context.call_sub_orchestrator_with_retry(
                "suborchestrator_rooftop_poly",
                retry,
                {**ingress},
            )
        ]
    )

    return {}

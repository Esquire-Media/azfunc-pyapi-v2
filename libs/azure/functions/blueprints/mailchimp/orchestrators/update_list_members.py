from azure.durable_functions import Blueprint, DurableOrchestrationContext

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_mailchimp_listMembers(context: DurableOrchestrationContext):
    ingress = context.get_input()

    return

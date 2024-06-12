from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from azure.durable_functions import Blueprint
import logging
import os

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_mailchimp_listMembers(context: DurableOrchestrationContext):
    ingress = context.get_input()
    
    
    
    return
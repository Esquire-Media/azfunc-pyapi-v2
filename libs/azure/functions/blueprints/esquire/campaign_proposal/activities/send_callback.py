from libs.azure.key_vault import KeyVaultClient
from libs.utils.email import send_email
import os
from libs.azure.functions import Blueprint
from azure.durable_functions import (
    DurableOrchestrationClient,
)


# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="ingress")
def activity_campaignProposal_generateCallback(ingress: dict):
    
    return "Test <b>Message</b> Body"
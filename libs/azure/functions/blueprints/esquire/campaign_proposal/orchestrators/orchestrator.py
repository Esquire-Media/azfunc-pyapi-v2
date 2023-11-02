from libs.azure.functions import Blueprint
from azure.durable_functions import (
    DurableOrchestrationContext,
    RetryOptions
)
import os
import logging

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_campaignProposal_root(context: DurableOrchestrationContext):
    
    # load payload information from orchestration context
    settings = context.get_input()
    
    # commonly used variables
    logging.warning(settings)
    taskHubName = str(context.get_input())
    retry = RetryOptions(15000, 1)
    egress = {
        "taskHubName": taskHubName,
        "instance_id": context.instance_id,
        **settings
    }

    # call activity to collect geocode addresses and get latlongs
    yield context.call_activity_with_retry(
        "activity_campaignProposal_geocodeAddresses",
        retry,
        {
            **egress
        },
    )

    # call activity to collect mover counts for each individual location as well as a deduped total
    yield context.call_activity_with_retry(
        "activity_campaignProposal_collectMovers",
        retry,
        {
            **egress
        }
    )

    # call activity to collect nearby competitors to each location
    yield context.call_activity_with_retry(
        "activity_campaignProposal_collectCompetitors",
        retry,
        {
            **egress
        }
    )

    # call activity to populate the PPTX report template and upload it as bytes to Azure storage
    yield context.call_activity_with_retry(
        'activity_campaignProposal_executeReport',
        retry,
        {
            **egress
        }
    )

    # generate the message body for the callback email
    message_body = yield context.call_activity_with_retry(
        "activity_campaignProposal_generateCallback",
        retry,
        {
            **egress
        },
    )

    # send the callback email
    yield context.call_activity_with_retry(
        "activity_microsoftGraph_sendEmail",
        retry,
        {
            "from_id": os.environ["O365_EMAIL_ACCOUNT_ID"],
            "to_addresses": [settings['callback']],
            "subject": f"Campaign Proposal: {settings['name']}",
            "message": message_body,
            "content_type": "HTML",
        },
    )

    logging.warning("All tasks completed.")

    return {}

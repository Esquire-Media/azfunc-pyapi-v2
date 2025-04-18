from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
import logging, os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_campaignProposal_root(context: DurableOrchestrationContext):
    try:
        # load payload information from orchestration context
        settings = context.get_input()

        # if a campaign proposal conn string is set, use that. Otherwise use AzureWebJobsStorage
        conn_str = (
            "CAMPAIGN_PROPOSAL_CONN_STR"
            if "CAMPAIGN_PROPOSAL_CONN_STR" in os.environ.keys()
            else "AzureWebJobsStorage"
        )

        # commonly used variables
        retry = RetryOptions(15000, 1)
        egress = {
            **settings,
            "instance_id": context.instance_id,
            "resources_container": {  # container for prebuilt assets
                "conn_str": conn_str,
                "container_name": "campaign-proposal-resources",
            },
            "runtime_container": {  # container for files generated during runtime, including the final report(s)
                "conn_str": conn_str,
                "container_name": "campaign-proposal",
            },
        }

        # call activity to collect geocode addresses and get latlongs
        yield context.call_activity_with_retry(
            "activity_campaignProposal_geocodeAddresses",
            retry,
            egress,
        )

        
        # call activity to collect mover counts for each individual location as well as a deduped total
        yield context.call_activity_with_retry(
            "activity_campaignProposal_collectMovers",
            retry,
            egress,
        )

        
        # call activity to collect nearby competitors to each location
        yield context.call_activity_with_retry(
            "activity_campaignProposal_collectCompetitors",
            retry,
            egress,
        )

        # call activity to populate the PPTX report template and upload it as bytes to Azure storage
        yield context.call_activity_with_retry(
            "activity_campaignProposal_executeReport",
            retry,
            egress,
        )

        # generate the message body for the callback email
        message_body = yield context.call_activity_with_retry(
            "activity_campaignProposal_generateCallback",
            retry,
            egress,
        )

        # send the callback email
        yield context.call_activity_with_retry(
            "activity_microsoftGraph_sendEmail",
            retry,
            {
                "from_id": os.environ["O365_EMAIL_ACCOUNT_ID"],
                "to_addresses": [settings["callback"]],
                "subject": f"Campaign Proposal: {settings['name']}",
                "message": message_body,
                "content_type": "HTML",
            },
        )

    except Exception as e:
        # if any errors are caught, post an error card to teams tagging Ryan and the calling user
        yield context.call_activity(
            "activity_microsoftGraph_postErrorCard",
            {
                "function_name": "esquire-campaign-proposal",
                "instance_id": context.instance_id,
                "owners": [egress["user"]],
                "error": f"{type(e).__name__} : {e}"[:1000],
                "webhook": os.environ["EXCEPTIONS_WEBHOOK_DEVOPS"],
            },
        )
        logging.warning("Error card sent")
        raise e

    logging.warning("All tasks completed.")

    # Purge history related to this instance
    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )

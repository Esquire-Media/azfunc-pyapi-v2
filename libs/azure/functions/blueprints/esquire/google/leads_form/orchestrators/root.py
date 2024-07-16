from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
import logging, os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_googleLeadsForm(context: DurableOrchestrationContext):
    """
    Orchestrate the ingestion, email callback, and error handling for Google Forms Leads.
    """

    try:
        # commonly used variables
        ingress = context.get_input()
        retry = RetryOptions(15000, 1)

        # build HTML email content
        email_content = yield context.call_activity_with_retry(
            "activity_googleLeadsForm_generateCallback", retry, {**ingress}
        )

        # get recipients from table based on formID
        recipients = yield context.call_activity_with_retry(
            "activity_googleLeadsForm_getRecipients",
            retry,
            {"form_id": ingress["form_id"]},
        )

        # send email with Lead information
        yield context.call_activity_with_retry(
            "activity_microsoftGraph_sendEmail",
            retry,
            {
                "from_id": os.environ["O365_EMAIL_ACCOUNT_ID"],
                "to_addresses": recipients,
                "subject": "New Lead from Esquire Advertising",
                "message": email_content,
                "content_type": "HTML",
            },
        )

    except Exception as e:
        # if any errors are caught, post an error card to teams tagging Ryan and the calling user
        yield context.call_activity(
            "activity_microsoftGraph_postErrorCard",
            {
                "function_name": "esquire-google-leads",
                "instance_id": context.instance_id,
                "owners": ["8489ce7c-e89f-4710-9d34-1442684ce7fe"],
                "error": f"{type(e).__name__} : {e}"[:1000],
                "webhook": os.environ["EXCEPTIONS_WEBHOOK_DEVOPS"],
            },
        )
        logging.warning("Error card sent")
        raise e

    # Purge history related to this instance
    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )

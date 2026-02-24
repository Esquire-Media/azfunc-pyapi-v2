import traceback
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
        full_trace = traceback.format_exc()
        html_body = f"""
            <html>
                <body>
                    <h2 style="color:red;">Google Leads Failure</h2>
                    <p><strong>{type(e).__name__}:</strong> {str(e)}</p>
                    <p><strong>Trace:</strong> {full_trace}</p>
                </body>
            </html>
            """
        yield context.call_activity(
            "activity_microsoftGraph_sendEmail",
            {
                "from_id": "57d355d1-eeb7-45a0-a260-00daceea9f5f",
                "to_addresses": ["matt@esquireadvertising.com"],
                "subject": "esquire-google-leads Failure",
                "message": html_body,
                "content_type": "html",
            },
        )
        raise e

    # Purge history related to this instance
    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )

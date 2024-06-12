from azure.durable_functions import Blueprint
import httpx

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_microsoft_graph_message_team_channel(ingress: dict):
    """
    Send a message to a Microsoft Teams channel using Microsoft Graph API.

    Parameters
    ----------
    ingress : dict
        Dictionary containing details for the message and destination.
        Expected keys are:
        - webhook: str, URL of the Microsoft Team's Channel Webhook.
        - message: str, The content of the message to be sent.

    Returns
    -------
    str
        Response text from the Microsoft Graph API request.

    Raises
    ------
    Exception
        If there's an error during the token acquisition or in the response from Microsoft Graph API.

    Examples
    --------
    Within an orchestrator function:

    >>> def orchestrator_function(context):
    ...     response = yield context.call_activity(
    ...         'activity_microsoft_graph_message_team_channel',
    ...         {
    ...             'webhook': 'https://***.webhook.office.com/webhookb2/***@***/IncomingWebhook/***/***',
    ...             'message': 'Hello, Teams!'
    ...         }
    ...     )
    """

    # Send email using Microsoft Graph
    return httpx.post(
        url=ingress["webhook"],
        json={"text": ingress["message"]},
    ).text

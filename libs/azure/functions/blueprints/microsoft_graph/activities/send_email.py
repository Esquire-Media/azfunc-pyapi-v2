import msal
import httpx
import os
from libs.azure.functions import Blueprint
from azure.durable_functions import (
    DurableOrchestrationClient,
)

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function that logs a message
@bp.activity_trigger(input_name="ingress")
def activity_microsoftGraph_sendEmail(ingress: dict):
    """
    Params:

    from_id         : The MS Graph user id of the sending user.
    to_addresses    : A list of email addresses to send to.
    subject         : Email subject line.
    message         : Email message body.
    content_type    : Content type of email. Acceptable values include "text" and "HTML".
    """

    # Generate an access token using MS authentication

    # Set up the Microsoft Authentication Library (MSAL) application
    result = msal.ConfidentialClientApplication(
        # Get the client ID from the environment variables
        client_id=os.getenv("MSGRAPH_CLIENT_ID"),
        # Construct the authority URL using the tenant ID from the environment variables
        authority="https://login.microsoftonline.com/" + os.getenv("MSGRAPH_TENANT_ID"),
        # Get the client secret from the environment variables
        client_credential=os.getenv("MSGRAPH_CLIENT_SECRET"),
        # Initialize the token cache
        token_cache=msal.TokenCache(),
    ).acquire_token_for_client(
        scopes=[".default"]
    )  # Acquire a token for the client

    # Check if the access token is present in the result
    if "access_token" in result:
        # Extract the access token from the result
        access_token = result["access_token"]
    else:
        # Raise an exception if there is an error in the response
        raise Exception(
            [
                "Graph Query Error:",
                result.get("error"),
                result.get("error_description"),
                result.get("correlation_id"),
            ]
        )

    # Send email using Microsoft Graph
    return httpx.post(
        url=f"https://graph.microsoft.com/v1.0/users/{ingress['from_id']}/sendMail",
        headers={
            "Authorization": "Bearer " + access_token,
            # "Content-Type": "application/json",
        },
        json={
            "message": {
                "subject": ingress["subject"],
                "body": {
                    "contentType": ingress["content_type"],
                    "content": ingress["message"],
                },
                "toRecipients": [
                    {"EmailAddress": {"address": address}}
                    for address in ingress["to_addresses"]
                ],
            },
            "saveToSentItems": "false",
        },
    ).text
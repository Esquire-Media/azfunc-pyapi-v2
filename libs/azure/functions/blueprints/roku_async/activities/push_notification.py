# File: libs/azure/functions/blueprints/async_tasks/activities/push_notification.py

from libs.azure.functions import Blueprint
from libs.azure.key_vault import KeyVaultClient
from libs.utils.email import send_email
import os

try:
    import simplejson as json
except:
    import json

bp = Blueprint()


# Define an Azure Durable Activity Function
@bp.activity_trigger(input_name="message")
def roku_async_activity_push_notification(message: dict):
    """
    Send a notification email using an Azure Durable Activity Function.

    This function sends an email with the given message by using the access
    token fetched from the Azure KeyVault. It uses the environment variables
    "KEY_VAULT" and "O365_EMAIL_ACCOUNT_ID" to fetch the access token and the
    from_id respectively.

    Parameters
    ----------
    message : dict
        The message to be sent via email. This message will be converted to a
        JSON string before sending.

    Returns
    -------
    None
    """

    send_email(
        # Fetch the access token from Azure KeyVault
        access_token=KeyVaultClient(os.environ["KEY_VAULT"])
        .get_secret("email-access")
        .value,
        # Fetch the sender email id from environment variables
        from_id=os.environ["O365_EMAIL_ACCOUNT_ID"],
        # The recipient email addresses
        to_addresses=["isaac@esquireadvertising.com"],
        # Subject of the email
        subject="Notification",
        # Convert the dictionary message to a JSON string
        message=json.dumps(message),
    )
    return None

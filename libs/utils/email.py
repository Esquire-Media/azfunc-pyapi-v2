from typing import List
import requests


def send_email(
    access_token: str, from_id: str, to_addresses: List[str], subject: str, message: str
):
    email_msg = {
        "Message": {
            "Subject": subject,
            "Body": {"ContentType": "Text", "Content": message},
            "ToRecipients": [
                {"EmailAddress": {"Address": address}} for address in to_addresses
            ],
        },
        "SaveToSentItems": "false",
    }
    return requests.post(
        f"https://graph.microsoft.com/v1.0/users/{from_id}/sendMail",
        headers={"Authorization": "Bearer " + access_token},
        json=email_msg,
    )

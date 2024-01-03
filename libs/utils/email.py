from typing import List
import requests


def send_email(
    access_token: str, from_id: str, to_addresses: List[str], subject: str, message: str, content_type:str="Text"
):
    email_msg = {
        "Message": {
            "Subject": subject,
            "Body": {"ContentType": content_type, "Content": message},
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

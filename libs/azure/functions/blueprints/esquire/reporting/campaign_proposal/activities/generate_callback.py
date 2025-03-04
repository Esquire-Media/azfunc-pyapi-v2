from azure.durable_functions import Blueprint
from azure.storage.blob import BlobClient
from datetime import timedelta
from libs.utils.azure_storage import get_blob_sas
import os

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_campaignProposal_generateCallback(settings: dict):

    # get a 14-day download URL for each file attachment
    url_pptx = get_blob_sas(
        blob=BlobClient.from_connection_string(
            conn_str=os.environ[settings["runtime_container"]["conn_str"]], 
            container_name=settings["runtime_container"]['container_name'],
            blob_name=f"{settings['instance_id']}/CampaignProposal-{settings['name']}.pptx"
        ),
        expiry=timedelta(days=14)
    )
    url_comps = get_blob_sas(
        blob=BlobClient.from_connection_string(
            conn_str=os.environ[settings["runtime_container"]["conn_str"]], 
            container_name=settings["runtime_container"]['container_name'],
            blob_name=f"{settings['instance_id']}/Competitors-{settings['name']}.xlsx"
        ),
        expiry=timedelta(days=14)
    )

    # build the message body including hyperlinks for each file download
    content = f"""Your Campaign Proposal report for <u>{settings['name']}</u> is done processing and ready for download. 
    <br><br>The following download link(s) will expire in 14 days:"""
    content += f"""<br><a href="{url_pptx}">Campaign Proposal-{settings['name']}.pptx</a>"""
    if "in_market_shopper" in settings.get("optional_slides", []):
        content += f"""<br><a href="{url_comps}">Competitors-{settings['name']}.xlsx</a>"""

    return content
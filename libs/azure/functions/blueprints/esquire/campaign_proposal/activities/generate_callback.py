import os
from datetime import datetime as dt, timedelta
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from libs.azure.functions import Blueprint

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_campaignProposal_generateCallback(settings: dict):

    # get a 14-day download URL for each file attachment
    url_pptx = get_blob_download_url(container_name='campaign-proposals', blob_name=f"{settings['instance_id']}/CampaignProposal-{settings['name']}.pptx")
    url_comps = get_blob_download_url(container_name='campaign-proposals', blob_name=f"{settings['instance_id']}/Competitors-{settings['name']}.xlsx")

    # build the message body including hyperlinks for each file download
    content = f"""Your Campaign Proposal report for <u>{settings['name']}</u> is done processing and ready for download. 
    <br><br>The following download link(s) will expire in 14 days:"""
    content += f"""<br><a href="{url_pptx}">Campaign Proposal-{settings['name']}.pptx</a>"""
    content += f"""<br><a href="{url_comps}">Competitors-{settings['name']}.xlsx</a>"""

    return content


def get_blob_sas(account_name,account_key, container_name, blob_name, expire_after=48):
    """
    Generates an expiring SAS token for the storage account containing report outputs.
    """
    sas_blob = generate_blob_sas(
        account_name=account_name, 
        container_name=container_name,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=dt.utcnow() + timedelta(hours=expire_after)
    )
    return sas_blob

def get_blob_download_url(container_name, blob_name):
    """
    Returns a secure download link to the finished report that will expire after 48 hours.
    """
    config = {}
    for c in os.environ['AzureWebJobsStorage'].split(';'):
        config[c[0:c.index("=")]] = c[c.index("=")+1:]

    sas_token = get_blob_sas(
        account_name=config['AccountName'],
        account_key=config['AccountKey'],
        container_name=container_name,
        blob_name=blob_name,
        expire_after=336  # 14 days
    )
    url = f"https://{config['AccountName']}.blob.{config['EndpointSuffix']}/{container_name}/{blob_name}?{sas_token}"
    return url
import os
from datetime import timedelta
from azure.storage.blob import BlobClient
from azure.durable_functions import Blueprint
from libs.utils.azure_storage import get_blob_sas

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_locationInsights_generateCallback(settings: dict):
      
    # get a 14-day download URL for output.pptx blob and add it to the message body
    content = f"""Your Location Insights Reports are done processing and ready for download. 
    <br><br>The following download link(s) will expire in 14 days:"""
    for output_blob_name in settings["output_blob_names"]:
        url_pptx = get_blob_sas(
            blob=BlobClient.from_connection_string(
                container_name=settings["runtime_container"]['container_name'],
                conn_str=os.environ[settings["runtime_container"]["conn_str"]], 
                blob_name=output_blob_name,
            ),
            expiry=timedelta(days=14)
        )
        content += f"""<br><a href="{url_pptx}">{output_blob_name.split('/')[-1]}</a>"""

    return content
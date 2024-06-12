# File: libs/azure/functions/blueprints/esquire/dashboard/meta/activities/download.py

from azure.storage.blob import BlobClient
from azure.durable_functions import Blueprint
from libs.openapi.clients.facebook import Facebook
import os, pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def esquire_dashboard_meta_activity_download(ingress: dict) -> dict:
    """
    Download data from Facebook using the Facebook API and upload it to an Azure Blob Storage.

    This function creates a request to download a report from Facebook, converts the report to a
    DataFrame, and then uploads it to an Azure Blob Storage container as a parquet file.

    Parameters
    ----------
    ingress : dict
        A dictionary containing the necessary parameters to execute the download and upload process.
        Expected keys are:
        - 'report_run_id': The ID of the report to download from Facebook.
        - 'conn_str': The name of the environment variable containing the connection string to Azure Blob Storage.
        - 'container_name': The name of the container in Azure Blob Storage where the file will be uploaded.
        - 'blob_name': The name of the blob (file) to be created in Azure Blob Storage.

    Returns
    -------
    str
        An empty string, indicating successful completion of the function.

    """
    # Initialize the Facebook API client
    factory = Facebook["Download"]

    # Set the access token for API security, preferring the ingress token, then environment variable
    factory.security.setdefault(
        "access_token",
        os.environ.get(
            ingress.get("access_token", ""), os.environ.get("META_ACCESS_TOKEN", "")
        ),
    )

    # Create and send a request to download the report from Facebook
    try:
        _, report, raw = factory.request(
            parameters={
                "name": "report",
                "format": "csv",
                "report_run_id": ingress["report_run_id"],
            }
        )
    except Exception as e:
        return {"success": False, "message": "Download Timeout"}

    # Check if the report contains any data
    if list(report.keys()):
        # Set up the BlobClient using the provided connection string and parameters
        blob: BlobClient = BlobClient.from_connection_string(
            conn_str=os.environ[ingress["conn_str"]],
            container_name=ingress["container_name"],
            blob_name=ingress["blob_name"],
        )

        # Upload the DataFrame as a parquet file to Azure Blob Storage, overwriting if it already exists
        blob.upload_blob(
            pd.DataFrame(report).to_parquet(index=False, compression="snappy"),
            overwrite=True,
        )
    else:
        return {"success": False, "message": raw.text.strip()}

    return {"success": True}

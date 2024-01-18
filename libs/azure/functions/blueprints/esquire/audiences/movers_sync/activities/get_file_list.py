# File: libs/azure/functions/blueprints/esquire/audiences/mover_sync/activities/get_file_list.py

from azure.storage.blob import ContainerClient
from libs.azure.functions import Blueprint
from libs.azure.key_vault import KeyVaultClient
from libs.utils.s3 import s3_path_to_azure_path, is_fresher_than_six_months
import boto3, os

bp = Blueprint()


@bp.activity_trigger(input_name="settings")
def activity_moversSync_getFileList(settings: dict):
    """
    Identifies a list of files in AWS S3 to be copied to Azure Blob Storage.

    Connects to AWS S3 and Azure Blob Storage, then iterates through specified S3 folders
    to find files that are not already in Azure Storage and are fresher than six months.

    Parameters
    ----------
    settings : dict
        A dictionary containing settings for S3 and Azure Blob Storage connections. 
        The expected keys and their values include:
        - 'runtime_container': A dictionary with details for the Azure Blob Storage container. It should contain:
            - 'conn_str': The name of the environment variable holding the Azure Blob Storage connection string.
            - 'container_name': The name of the Azure Blob container where files are to be copied.

    Returns
    -------
    list
        A list of file paths in S3 that need to be copied to Azure Blob Storage.
    """
    
    # Connect to AWS S3 using credentials from Azure Key Vault
    s3_key_vault = KeyVaultClient("s3-service")
    session = boto3.Session(
        aws_access_key_id=s3_key_vault.get_secret("access-key-id").value,
        aws_secret_access_key=s3_key_vault.get_secret("secret-access-key").value,
    )
    s3 = session.resource("s3")
    bucket = s3.Bucket("esquire-movers")

    # Connect to Azure Storage and get a list of existing blob names
    container = ContainerClient.from_connection_string(
        conn_str=os.environ[settings["runtime_container"]["conn_str"]],
        container_name=settings["runtime_container"]["container_name"],
    )
    blobs_in_azure = [blob.name for blob in container.list_blobs()]

    # Identify files in S3 to copy to Azure Storage
    files_to_copy = []
    s3_folders = [
        "movers-3-month-segment-partitioned",  # 3 recent months of movers
        "archived-data/archive movers-3-month-segment-partitioned",  # archived movers
        "AVRICK-premovers",  # recent premovers file
        "archived-data/archive premovers",  # archived premovers
    ]
    for object in bucket.objects.all():
        if any(object.key.startswith(folder) for folder in s3_folders):
            if len(os.path.basename(object.key)) > 1:
                try:
                    if s3_path_to_azure_path(object.key) not in blobs_in_azure:
                        files_to_copy.append(object.key)
                except TypeError:
                    continue

    # Filter for files fresher than six months
    files_to_copy = [file for file in files_to_copy if is_fresher_than_six_months(file)]

    return files_to_copy

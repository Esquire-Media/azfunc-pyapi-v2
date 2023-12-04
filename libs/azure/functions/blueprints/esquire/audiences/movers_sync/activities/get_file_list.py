import logging
from libs.azure.functions import Blueprint
import boto3
import os
from libs.utils.s3 import s3_path_to_azure_path, is_fresher_than_six_months
from azure.storage.blob import ContainerClient
from libs.azure.key_vault import KeyVaultClient

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_moversSync_getFileList(settings: dict):

    # connect to an AWS boto3 session
    s3_key_vault = KeyVaultClient('s3-service')
    session = boto3.Session(
        aws_access_key_id=s3_key_vault.get_secret('access-key-id').value,
        aws_secret_access_key=s3_key_vault.get_secret('secret-access-key').value,
    )
    s3 = session.resource("s3")
    bucket = s3.Bucket("esquire-movers")

    # connect to Azure client and get a list of blob names already in storage
    container = ContainerClient.from_connection_string(conn_str=os.environ[settings['runtime_container']['conn_str']], container_name=settings['runtime_container']['container_name'])
    blobs_in_azure = [blob.name for blob in container.list_blobs()]

    # iterate through items in s3 bucket(s) and identify files to copy to Azure storage
    files_to_copy = []
    s3_folders = [
        "movers-3-month-segment-partitioned",                           # 3 most recent months of movers
        "archived-data/archive movers-3-month-segment-partitioned/"     # archived movers
        "AVRICK-premovers",                                             # single most recent premovers file
        "archived-data/archive premovers",                              # archived premovers
    ]
    for object in bucket.objects.all():
        # search the specific folders that hold the relevant data
        if any([object.key.startswith(folder) for folder in s3_folders]):
            # skip non-file endpoints
            if len(os.path.basename(object.key)) > 1:
                try:
                    # check that the file doesn't already exist on Azure
                    if s3_path_to_azure_path(object.key) not in blobs_in_azure:
                        files_to_copy.append(object.key)
                except TypeError:  # if path conversion fails, skip that object
                    continue

    # filter for files that are fresher than 24 weeks, based on the s3 filepath DDMMYYYY datestring
    files_to_copy = [file for file in files_to_copy if is_fresher_than_six_months(file)]

    logging.warning(f"{len(files_to_copy)} files to copy.")
    return files_to_copy

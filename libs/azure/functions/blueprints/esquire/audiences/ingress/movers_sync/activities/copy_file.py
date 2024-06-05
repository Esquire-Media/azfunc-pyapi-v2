import logging
import boto3
import os
from libs.utils.s3 import s3_path_to_azure_path
from smart_open import open as s_open
from libs.azure.functions import Blueprint
from azure.storage.blob import BlobServiceClient
from libs.azure.key_vault import KeyVaultClient
import shutil

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_moversSync_copyFile(settings: dict):
    
    # connect to s3 storage using boto3
    s3_key_vault = KeyVaultClient('s3-service')
    session = boto3.Session(
        aws_access_key_id=s3_key_vault.get_secret('access-key-id').value,
        aws_secret_access_key=s3_key_vault.get_secret('secret-access-key').value,
    )

    # blob service client
    bsc = BlobServiceClient.from_connection_string(os.environ[settings['runtime_container']['conn_str']])

    # copy the file from S3 to Azure blob storage
    s3_path = f"s3://esquire-movers/{settings['filepath']}"
    azure_path = (
        f"azure://{settings['runtime_container']['container_name']}/{s3_path_to_azure_path(settings['filepath'])}"
    )
    logging.warning(azure_path)
    # logging.warning(f"CopyFile: {azure_path}")
    # open s3 file-like object and an azure file-like object
    with s_open(
        s3_path, "rb", transport_params={"client": session.client("s3")}
    ) as fin, s_open(
        azure_path, "wb", transport_params={"client": bsc}
    ) as fout:
        # copy data between the file-like objects
        shutil.copyfileobj(fin, fout)

    return s3_path_to_azure_path(settings['filepath'])
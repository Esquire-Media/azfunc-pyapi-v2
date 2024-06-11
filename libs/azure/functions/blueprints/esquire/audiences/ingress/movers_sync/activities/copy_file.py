import logging
import os
import fsspec
from libs.utils.s3 import s3_path_to_azure_path
from libs.azure.functions import Blueprint
from libs.azure.key_vault import KeyVaultClient
import shutil

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()


# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_moversSync_copyFile(settings: dict):
    s3_key_vault = KeyVaultClient("s3-service")

    # Copy the file from S3 to Azure blob storage
    s3_path = f"s3://esquire-movers/{settings['filepath']}"
    azure_path = f"az://{settings['runtime_container']['container_name']}/{s3_path_to_azure_path(settings['filepath'])}"
    logging.warning(azure_path)

    # Open S3 file-like object and an Azure file-like object using fsspec
    s3_fs = fsspec.filesystem(
        "s3",
        key=s3_key_vault.get_secret("access-key-id").value,
        secret=s3_key_vault.get_secret("secret-access-key").value,
    )
    azure_fs = fsspec.filesystem(
        "az", connection_string=os.environ[settings["runtime_container"]["conn_str"]]
    )

    with s3_fs.open(s3_path, "rb") as fin, azure_fs.open(azure_path, "wb") as fout:
        # Copy data between the file-like objects
        shutil.copyfileobj(fin, fout)

    return s3_path_to_azure_path(settings["filepath"])

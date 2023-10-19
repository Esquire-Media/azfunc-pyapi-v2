# File: libs/azure/functions/blueprints/oneview/segments/activities/generate_onspot_header.py

from azure.storage.filedatalake import (
    FileSystemClient,
    FileSasPermissions,
    generate_file_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
import os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def oneview_segments_generate_onspot_header(ingress: dict):
    """
    Generate a header for OnSpot data and store it in Azure Data Lake.

    This function creates a CSV header for OnSpot data and uploads it to
    an Azure Data Lake. The function then returns a URL to the stored header
    in the Azure Data Lake.

    Parameters
    ----------
    ingress : dict
        Dictionary containing details about the Azure Data Lake storage.

    Returns
    -------
    dict
        A dictionary containing the URL to the stored header in Azure Data Lake.

    Notes
    -----
    This function uses the azure.storage.filedatalake library to interact
    with Azure Data Lake.
    """

    # Initialize Azure Data Lake client using connection string from environment variables
    filesystem: FileSystemClient = FileSystemClient.from_connection_string(
        conn_str=os.environ[ingress["output"]["conn_str"]],
        file_system_name=ingress["output"]["container_name"],
    )

    # Define the path in Azure Data Lake to store the header
    file = filesystem.get_file_client(
        file_path="{}/raw/{}".format(ingress["output"]["prefix"], "header.csv"),
    )

    # Upload the header data to Azure Data Lake
    file.upload_data(b"street,city,state,zip,zip4", overwrite=True)

    # Generate a SAS token for the stored header in Azure Data Lake and return the URL
    return {
        "url": (
            file.url
            + "?"
            + generate_file_sas(
                file.account_name,
                file.file_system_name,
                "/".join(file.path_name.split("/")[:-1]),
                file.path_name.split("/")[-1],
                filesystem.credential.account_key,
                FileSasPermissions(read=True),
                datetime.utcnow() + relativedelta(days=2),
            )
        ),
        "columns": None,
    }

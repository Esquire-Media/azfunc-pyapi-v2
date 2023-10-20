# File: libs/azure/functions/blueprints/esquire/audiences/oneview/activities/fetch_s3_data.py

from azure.storage.filedatalake import DataLakeFileClient
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from urllib.parse import unquote
import boto3, os, pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def esquire_audiences_oneview_fetch_s3_data(ingress: dict) -> dict:
    """
    Fetch data from S3, process it, store in Azure Data Lake, and return a SAS token URL.

    This function retrieves specified data from an S3 bucket, processes it,
    and then stores the processed data in Azure Data Lake. After storing, it
    generates a SAS token URL for accessing the data from the Data Lake.

    Parameters
    ----------
    ingress : dict
        A dictionary containing necessary parameters, specified as follows:
        - output (dict): Contains keys like "conn_str", "container_name", and "prefix" for Azure Data Lake.
        - s3_key (str): Key (path) of the object in the S3 bucket to be fetched.
        - record (dict): Contains the "Bucket" key specifying the S3 bucket name.

    Returns
    -------
    dict
        A dictionary containing:
        - url (str): The SAS token URL for accessing the stored data in Azure Data Lake.
        - columns (list): List of column names in the processed data.

    Examples
    --------
    In an orchestrator function:

    .. code-block:: python

        import azure.durable_functions as df

        def orchestrator_function(context: df.DurableOrchestrationContext):
            data_info = yield context.call_activity('esquire_audiences_oneview_fetch_s3_data', {
                "output": {
                    "conn_str": "AZURE_DATALAKE_CONNECTION_STRING",
                    "container_name": "my-container",
                    "prefix": "data_prefix"
                },
                "s3_key": "path/to/s3/object",
                "record": {
                    "Bucket": "my-s3-bucket"
                }
            })
            return data_info

    Notes
    -----
    - The function uses pandas to process the data in chunks.
    - It first determines the type of data (device or address) and processes it accordingly.
    - The processed data is then stored in Azure Data Lake.
    - A SAS token URL is generated for the stored data, which provides read access for two days.
    """

    # Define the path in Azure Data Lake to store the data
    file = DataLakeFileClient.from_connection_string(
        conn_str=os.environ[ingress["output"]["conn_str"]],
        file_system_name=ingress["output"]["container_name"],
        file_path="{}/raw/{}".format(
            ingress["output"]["prefix"], ingress["s3_key"].split("/")[-1]
        ),
    )
    file.create_file()

    # Initialize S3 client using credentials from environment variables
    s3 = boto3.Session(
        aws_access_key_id=os.environ["REPORTS_AWS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["REPOSTS_AWS_SECRET_KEY"],
        region_name=os.environ["REPORTS_AWS_REGION"],
    ).client("s3")

    # Fetch the object from S3
    obj = s3.get_object(Bucket=ingress["record"]["Bucket"], Key=ingress["s3_key"])

    # Initialize the offset for appending data in Azure Data Lake
    offset = 0
    columns = []

    # Read the S3 object data in chunks using pandas
    for index, chunk in enumerate(
        pd.read_csv(obj["Body"], chunksize=100000, encoding_errors="ignore")
    ):
        if "devices" in chunk.columns:
            chunk = chunk[["devices"]]
        elif "deviceid" in chunk.columns:
            chunk = chunk[["deviceid"]].rename(columns={"deviceid": "devices"})
        elif "deviceids" in chunk.columns:
            chunk = chunk[["deviceids"]].rename(columns={"deviceids": "devices"})
        else:
            if "address" in chunk.columns:
                chunk.rename(columns={"address": "street"}, inplace=True)
            if "zipcode" in chunk.columns:
                chunk.rename(columns={"zipcode": "zip"}, inplace=True)
            if "zip-4" in chunk.columns:
                chunk.rename(columns={"zip-4": "zip4"}, inplace=True)

            if "street" in chunk.columns:
                chunk["street"] = chunk["street"].str.strip()
            if "city" in chunk.columns:
                chunk["city"] = chunk["city"].str.strip()
            if "state" in chunk.columns:
                chunk["state"] = chunk["state"].str.strip()
            if "zip" in chunk.columns:
                chunk["zip"] = chunk["zip"].astype("int", errors="ignore")
                chunk["zip"] = chunk["zip"].astype("str")
                chunk["zip"] = chunk["zip"].str.zfill(5)
            if "zip4" in chunk.columns:
                chunk["zip4"] = chunk["zip4"].astype("int", errors="ignore")
                chunk["zip4"] = chunk["zip4"].astype("str")
                chunk["zip4"] = chunk["zip4"].str.zfill(4)

            if "zip" in chunk.columns and "zip4" not in chunk.columns:
                chunk["zip4"] = None

            chunk = chunk[["street", "city", "state", "zip", "zip4"]]
        if index == 0:
            columns = chunk.columns.to_list()
        # Append the processed chunk of data to Azure Data Lake
        data = chunk.to_csv(
            index=False,
            sep=",",
            lineterminator="\n",
            header=False,
            encoding="utf-8",
        )
        file.append_data(data=data, offset=offset)
        offset += len(data)

    # Flush the appended data to Azure Data Lake
    file.flush_data(offset=offset)

    # Generate a SAS token for the stored data in Azure Data Lake and return the URL
    blob = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["output"]["conn_str"]],
        container_name=ingress["output"]["container_name"],
        blob_name="{}/raw/{}".format(
            ingress["output"]["prefix"], ingress["s3_key"].split("/")[-1]
        ),
    )
    return {
        "url": (
            unquote(blob.url)
            + "?"
            + generate_blob_sas(
                account_name=blob.account_name,
                container_name=blob.container_name,
                blob_name=blob.blob_name,
                account_key=blob.credential.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + relativedelta(days=2),
            )
        ),
        "columns": columns,
    }

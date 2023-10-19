# File: libs/azure/functions/blueprints/oneview/segments/activities/fetch_audiences_s3_data.py

from azure.storage.filedatalake import (
    FileSystemClient,
    FileSasPermissions,
    generate_file_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
import boto3, os, pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def oneview_segments_fetch_audiences_s3_data(ingress: dict):
    """
    Fetch audience data from S3 and store it in Azure Data Lake.

    This function retrieves audience data from an S3 bucket based on a given key,
    processes and transforms the data, and then stores it in an Azure Data Lake.
    The function returns a URL to the stored data in the Azure Data Lake.

    Parameters
    ----------
    ingress : dict
        Dictionary containing details about the S3 bucket, key, and Azure Data Lake storage.

    Returns
    -------
    dict
        A dictionary containing the URL to the stored data in Azure Data Lake and the columns of the data.

    Notes
    -----
    This function uses the boto3 library to interact with AWS S3 and the
    azure.storage.filedatalake library to interact with Azure Data Lake.
    """

    # Initialize Azure Data Lake client using connection string from environment variables
    filesystem: FileSystemClient = FileSystemClient.from_connection_string(
        conn_str=os.environ[ingress["output"]["conn_str"]],
        file_system_name=ingress["output"]["container_name"],
    )
    # Define the path in Azure Data Lake to store the data
    file = filesystem.get_file_client(
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
        ).replace("https://", "az://"),
        "columns": columns,
    }

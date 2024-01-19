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
def activity_oneview_fetchS3Data(ingress: dict) -> dict:
    """
    Fetch data from an S3 bucket and store it in Azure Data Lake.

    This function retrieves data from an S3 bucket, processes it using pandas, and stores the processed data
    in Azure Data Lake. The final data can be accessed through a generated SAS token URL.

    Parameters
    ----------
    ingress : dict
        Configuration details for fetching and storing data:
        - source (dict): Details for the S3 data source.
            - access_key (str): AWS access key ID.
            - secret_key (str): AWS secret access key.
            - region (str): AWS region.
            - bucket (str): S3 bucket name.
            - key (str): S3 object key.
        - target (dict): Details for the Azure Data Lake target.
            - conn_str (str): Connection string name for Azure Data Lake.
            - container_name (str): Container (or file system) name in Azure Data Lake.
            - prefix (str): Prefix for the Azure Data Lake path.

    Returns
    -------
    dict
        - url (str): SAS token URL to access the stored data in Azure Data Lake.
        - columns (list): List of columns present in the fetched data.

    Examples
    --------
    In an orchestrator function:

    .. code-block:: python

        import azure.durable_functions as df

        def orchestrator_function(context: df.DurableOrchestrationContext):
            result = yield context.call_activity('activity_oneview_fetchS3Data', {
                "source": {
                    "access_key": "YOUR_AWS_ACCESS_KEY",
                    "secret_key": "YOUR_AWS_SECRET_KEY",
                    "region": "YOUR_AWS_REGION",
                    "bucket": "your-s3-bucket",
                    "key": "path/to/your/s3/object.csv"
                },
                "target": {
                    "conn_str": "YOUR_AZURE_CONNECTION_STRING",
                    "container_name": "your-datalake-container",
                    "prefix": "your/prefix/for/storing"
                }
            })
            return result

    Notes
    -----
    - The function uses pandas to read the S3 data in chunks and process it.
    - The data is then appended to Azure Data Lake using the Azure SDK's `DataLakeFileClient`.
    - The generated SAS token URL provides read access to the stored data in Azure Data Lake.
    """

    # Define the path in Azure Data Lake to store the data
    file = DataLakeFileClient.from_connection_string(
        conn_str=os.environ[ingress["target"]["conn_str"]],
        file_system_name=ingress["target"]["container_name"],
        file_path="{}/{}".format(
            ingress["target"]["prefix"], ingress["source"]["key"].split("/")[-1]
        ),
    )
    if file.exists():
        file.delete_file()
    file.create_file()

    # Initialize S3 client using credentials from environment variables
    s3 = boto3.Session(
        aws_access_key_id=os.getenv(
            ingress["source"]["access_key"], ingress["source"]["access_key"]
        ),
        aws_secret_access_key=os.getenv(
            ingress["source"]["secret_key"], ingress["source"]["secret_key"]
        ),
        region_name=os.getenv(ingress["source"]["region"], ingress["source"]["region"]),
    ).client("s3")

    # Fetch the object from S3
    obj = s3.get_object(
        Bucket=ingress["source"]["bucket"], Key=ingress["source"]["key"]
    )

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
        conn_str=os.environ[ingress["target"]["conn_str"]],
        container_name=ingress["target"]["container_name"],
        blob_name="{}/{}".format(
            ingress["target"]["prefix"], ingress["source"]["key"].split("/")[-1]
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

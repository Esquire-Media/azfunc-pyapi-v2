from azure.storage.blob import BlobClient
from datetime import timedelta
from azure.durable_functions import Blueprint
import pandas as pd, os, logging
from uuid import uuid4
from libs.utils.azure_storage import get_blob_sas
from libs.utils.text import (
    format_zipcode,
    format_sales,
    format_date,
)


bp: Blueprint = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_salesUploader_salesPreProcessing(ingress: dict):
    """
    Processes sales data by performing cleaning and standardization steps before running Smarty validation.

    Overview:
    This function takes sales data from an ingress blob, cleans and formats it by removing null rows and columns, adding a unique transaction ID, 
        and standardizing certain fields such as zip codes, addresses, and sales amounts. 
    It also handles date standardization and fills missing date values if specified. 
    Non-standardizable columns are separated into an appendix and uploaded to a specified blob container. 
    Finally, the processed data is uploaded to an egress blob for further processing.

    Parameters:
    - ingress (dict): A dictionary containing configuration and settings for the function. Expected keys include:
      - 'runtime_container': Information for connecting to the source Azure Blob storage, including connection string, container name, and blob name for reading raw sales data.
      - 'uploads_container': Information for connecting to the destination Azure Blob storage for uploading the appendix data.
      - 'settings': Processing settings such as 'matchback_name', 'group_id', 'timestamp', and optional 'date_fill' for filling missing dates.
      - 'columns': A mapping of column names to standardize from the source data to a predefined nameset.
      - 'instance_id': A unique identifier for the instance of data processing, used in naming blobs.
      - 'timestamp': A timestamp indicating when the processing is initiated, used in naming the appendix blob.

    Returns:
    - egress: A dictionary containing connection information for the egress blob where the processed data is uploaded. Keys include 'conn_str', 'container_name', and 'blob_name'.
    """

    logging.warning("activity_salesUploader_salesPreProcessing")

    # connect to the ingress sales blob
    ingress_client = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["runtime_container"]["conn_str"]],
        container_name=ingress["runtime_container"]["container_name"],
        blob_name=f"{ingress['instance_id']}/01_ingress",
    )
    df = pd.read_csv(
        get_blob_sas(blob=ingress_client, expiry=timedelta(minutes=10)), dtype=str
    )
    df = df.dropna(axis=1, how='all') # drop null columns
    df = df.dropna(axis=0, how='all') # drop null rows
    df.insert(loc=0, column='transactionId', value=[str(uuid4()) for _ in range(len(df.index))]) # set a unique ID for each transaction
    df['matchbackName'] = ingress['settings']['matchback_name']

    # slice to only include standardizable columns, and rename to the standard column nameset
    standardizable = df[[col for col in df.columns if col in ingress["columns"].values() or col in ['transactionId']]]
    standardizable = standardizable.rename(columns={v: k for k, v in ingress["columns"].items()})

    # non-standardizable columns will be stored as a data appendix, with transactionId as a shared index
    appendix = df[[col for col in df.columns if col not in ingress["columns"].values() or col in ['transactionId']]]
    appendix = appendix.melt(id_vars=['transactionId'])

    # upload the appendix data to its final destination (no more processing is required on it)
    with BlobClient.from_connection_string(
        conn_str=os.environ[ingress['uploads_container']['conn_str']],
        container_name=ingress['uploads_container']["container_name"],
        blob_name=f"{ingress['settings']['group_id']}/{ingress['timestamp']}.appendix",
    ) as appendix_client:
        appendix_client.upload_blob(appendix.to_parquet())

    # fill date values if not set or null values exist
    if "date_fill" in ingress["settings"].keys():
        if "date" in standardizable.columns:
            standardizable["date"].fillna(ingress["settings"]["date_fill"])
        else:
            standardizable["date"] = ingress["settings"]["date_fill"]
    standardizable["date"] = standardizable["date"].apply(format_date)

    # format the zipcodes to ensure standardization
    standardizable["zipcode"] = standardizable["zipcode"].apply(format_zipcode)
    standardizable = standardizable.dropna(subset=["zipcode"])

    # uppercase the text columns to prevent case matching issues later
    standardizable["address"] = standardizable["address"].str.upper()
    if 'city' in standardizable.columns:
        standardizable["city"] = standardizable["city"].str.upper()
    if 'state' in standardizable.columns:
        standardizable["state"] = standardizable["state"].str.upper()

    # validate the sales column, or create one if it doesn't exist yet
    if "saleAmount" not in standardizable.columns:
        standardizable["saleAmount"] = 0
    else:
        standardizable["saleAmount"] = standardizable["saleAmount"].apply(format_sales)

    # connect to the output blob and upload the processed data
    egress = {
        "conn_str":ingress['runtime_container']['conn_str'],
        "container_name":ingress['runtime_container']["container_name"],
        "blob_name":f"{ingress['instance_id']}/02_preprocessed"
    }
    egress_client = BlobClient.from_connection_string(
        conn_str=os.environ[egress["conn_str"]],
        container_name=egress["container_name"],
        blob_name=egress['blob_name'],
    )
    egress_client.upload_blob(standardizable.to_csv(index=False))

    return egress
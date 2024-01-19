from azure.storage.blob import BlobClient
from datetime import timedelta
from libs.azure.functions import Blueprint
from libs.utils.smarty import bulk_validate
import pandas as pd, os, logging
from uuid import uuid4
from libs.azure.storage.blob.sas import get_blob_download_url
from libs.utils.text import (
    format_zipcode,
    format_zip4,
    format_sales,
    format_date,
)


bp: Blueprint = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_salesUploader_salesPreProcessing(ingress: dict):

    logging.warning("activity_salesUploader_salesPreProcessing")

    # connect to the ingress sales blob
    ingress_client = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["runtime_container"]["conn_str"]],
        container_name=ingress["runtime_container"]["container_name"],
        blob_name=f"{ingress['instance_id']}/01_ingress",
    )
    df = pd.read_csv(
        get_blob_download_url(blob_client=ingress_client, expiry=timedelta(minutes=10))
    )
    df = df.dropna(axis=1, how='all') # drop null columns
    df = df.dropna(axis=0, how='all') # drop null rows
    df.insert(loc=0, column='transactionId', value=[uuid4() for _ in range(len(df.index))]) # set a unique ID for each transaction

    # slice to only include standardizable columns, and rename to the standard column nameset
    standardizable = df[[col for col in df.columns if col in ingress["columns"].values() or col in ['transactionId']]]
    standardizable = standardizable.rename(columns={v: k for k, v in ingress["columns"].items()})

    # non-standardizable columns will be stored as a data appendix, with transactionId as a shared index
    appendix = df[[col for col in df.columns if col not in ingress["columns"].values() or col in ['transactionId']]]
    # upload the appendix data to its final destination (no more processing is required on it)
    appendix_client = BlobClient.from_connection_string(
        conn_str=os.environ[ingress['uploads_container']['conn_str']],
        container_name=ingress['uploads_container']["container_name"],
        blob_name=f"{ingress['settings']['group_id']}/{ingress['settings']['matchback_name']}/{ingress['instance_id']}.appendix",
    )
    appendix_client.upload_blob(appendix.to_csv(index=False))

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
    standardizable["city"] = standardizable["city"].str.upper()
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
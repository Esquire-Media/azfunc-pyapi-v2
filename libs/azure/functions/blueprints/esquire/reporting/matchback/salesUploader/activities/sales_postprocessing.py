from azure.storage.blob import BlobClient
from datetime import timedelta
from libs.azure.functions import Blueprint
from libs.utils.smarty import bulk_validate
import pandas as pd, os, logging
from libs.azure.storage.blob.sas import get_blob_download_url
from libs.utils.text import (
    format_zipcode,
    format_zip4,
    format_sales,
    format_date,
)

bp: Blueprint = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_salesUploader_salesPostProcessing(ingress: dict):

    logging.warning("activity_salesUploader_salesPostProcessing")

    # connect to the processed smarty addresses blob
    smarty_df = pd.read_csv(ingress["processed_blob_url"])
    
    # connect to the ingress sales blob
    ingress_client = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["runtime_container"]["conn_str"]],
        container_name=ingress["runtime_container"]["container_name"],
        blob_name=f"{ingress['instance_id']}/02_preprocessed",
    )
    ingress_df = pd.read_csv(
        get_blob_download_url(blob_client=ingress_client, expiry=timedelta(minutes=10))
    )

    # merge Smarty-validated data back onto the ingress dataset
    merged_df = pd.merge(
        smarty_df[
            ["delivery_line_1", "city_name", "state_abbreviation", "zipcode", "plus4_code"]
        ],
        ingress_df.drop(columns=["address", "city", "state", "zipcode"]),
        right_index=True,
        left_index=True,
    )
    merged_df['matchbackName'] = ingress['settings']['matchback_name']

    # connect to the output blob and upload the processed data
    egress = {
        "conn_str":ingress['uploads_container']['conn_str'],
        "container_name":ingress['uploads_container']["container_name"],
        "blob_name":f"{ingress['settings']['group_id']}/{ingress['instance_id']}.standardized",
        "date_first":merged_df['date'].min(),
        "date_last":merged_df['date'].max()
    }
    egress_client = BlobClient.from_connection_string(
        conn_str=os.environ[egress["conn_str"]],
        container_name=egress["container_name"],
        blob_name=egress['blob_name'],
    )
    egress_client.upload_blob(merged_df.to_parquet(index=False))

    return egress
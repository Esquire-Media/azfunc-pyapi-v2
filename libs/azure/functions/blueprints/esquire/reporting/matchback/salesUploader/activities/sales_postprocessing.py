from azure.storage.blob import BlobClient
from datetime import timedelta
from azure.durable_functions import Blueprint
import pandas as pd, os, logging
from libs.utils.azure_storage import get_blob_sas

bp: Blueprint = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_salesUploader_salesPostProcessing(ingress: dict):
    """
    Finalizes the sales data processing by merging validated address data and uploading the standardized dataset for further analysis.

    This function integrates address validation results from Smarty with the preprocessed sales data.
    It first reads the Smarty validated addresses and the preprocessed sales data from their respective blobs.
    Then, it merges these datasets, replacing the original address components with the validated ones.
    Finally, the merged dataset is uploaded to a specified Azure Blob storage container as a parquet file.
    The function also calculates and includes the earliest and latest date present in the data for reference.

    Parameters:
    - ingress (dict): A dictionary containing configuration and settings for the function. Expected keys include:
      - 'runtime_container': Information for connecting to the source Azure Blob storage to read the preprocessed sales data.
      - 'uploads_container': Information for connecting to the destination Azure Blob storage for uploading the finalized dataset.
      - 'processed_blob_url': The URL to the blob containing Smarty validated address data.
      - 'settings': Processing settings such as 'group_id' and 'timestamp', used in naming the output blob.
      - 'instance_id': A unique identifier for the instance of data processing, used in accessing the preprocessed data blob.

    Returns:
    - egress: A dictionary containing connection information for the egress blob where the finalized data is uploaded, along with the earliest and latest dates found in the data. Keys include 'conn_str', 'container_name', 'blob_name', 'date_first', and 'date_last'.
    """

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
        get_blob_sas(blob=ingress_client, expiry=timedelta(minutes=10))
    )

    # merge Smarty-validated data back onto the ingress dataset
    merged_df = pd.merge(
        smarty_df[
            [
                "delivery_line_1",
                "city_name",
                "state_abbreviation",
                "zipcode",
                "plus4_code",
            ]
        ],
        ingress_df.drop(
            columns=["address", "city", "state", "zipcode"], errors="ignore"
        ),
        right_index=True,
        left_index=True,
    )

    # connect to the output blob and upload the processed data
    egress = {
        "conn_str": ingress["uploads_container"]["conn_str"],
        "container_name": ingress["uploads_container"]["container_name"],
        "blob_name": f"{ingress['settings']['group_id']}/{ingress['timestamp']}.standardized",
        "date_first": merged_df["date"].min(),
        "date_last": merged_df["date"].max(),
    }
    with BlobClient.from_connection_string(
        conn_str=os.environ[egress["conn_str"]],
        container_name=egress["container_name"],
        blob_name=egress["blob_name"],
    ) as egress_client:
        egress_client.upload_blob(merged_df.to_parquet())

    return egress

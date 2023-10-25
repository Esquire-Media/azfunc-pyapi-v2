# File: libs/azure/functions/blueprints/esquire/dashboard/oneview/activities/partition.py

from azure.storage.blob import ContainerClient
from datetime import datetime
from libs.azure.functions import Blueprint
import os, pandas as pd


bp = Blueprint()

def upload_blob(client, blob_name, data):
    client.upload_blob(name=blob_name, data=data, overwrite=True)


@bp.activity_trigger(input_name="ingress")
def esquire_dashboard_oneview_activity_partition(ingress: dict):
    report = pd.read_csv(ingress["source"])
    # NOTE: Roku report has an anomalous colname 'Creative Uid' which doesn't match the typical naming schema. We change this to 'creative_uid'.
    report = report.rename(columns={"Creative Uid": "creative_uid"})

    # connect to the esquireroku storage container to upload data (partitioned by date)
    upload_client: ContainerClient = ContainerClient.from_connection_string(
        conn_str=os.environ[ingress["target"]["conn_str"]],
        container_name=ingress["target"]["container_name"],
    )
    blob_names = []
    for date_str, df in report.groupby("date"):
        # format blob name with datepath
        date = datetime.fromisoformat(date_str)
        blob_name = "{}year={}/month={:02}/day={:02}/data.parquet".format(
            ingress["target"].get("prefix", ""),
            date.year,
            date.month,
            date.day,
        )
        blob_names.append(blob_name)

        # export to storage as parq blob, overwrite existing
        upload_client.upload_blob(
            name=blob_name, data=df.to_parquet(index=False), overwrite=True
        )

    return blob_names  # not used

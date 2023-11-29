# File: libs/azure/functions/blueprints/esquire/onspot/activities/merge_devices.py

from libs.azure.functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    ContainerClient,
    ContainerSasPermissions,
    generate_container_sas,
)
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

bp: Blueprint = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_onSpot_mergeDevices(ingress: dict):
    # source container
    sources_container_client = ContainerClient.from_connection_string(
        conn_str=os.environ[ingress["source"]["conn_str"]],
        container_name=ingress["source"]["container_name"],
    )
    # generate sas token
    sas_token = generate_container_sas(
        account_name=sources_container_client.account_name,
        account_key=sources_container_client.credential.account_key,
        container_name=sources_container_client.container_name,
        permission=ContainerSasPermissions(write=True, read=True),
        expiry=datetime.utcnow() + relativedelta(days=2),
    )

    # make connection to the container
    destination_blob_client = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["destination"]["conn_str"]],
        container_name=ingress["destination"]["container_name"],
        blob_name=ingress["destination"]["blob_name"],
    )
    first = True

    for blob_name in sources_container_client.list_blob_names(
        name_starts_with=ingress["source"]["blob_prefix"]
    ):
        if not blob_name.endswith(".debug.csv"):
            if first:
                destination_blob_client.create_append_blob()
                header = (
                    sources_container_client.download_blob(blob_name, 0, 8).read()
                    == b"deviceid"
                )
                first = False

            destination_blob_client.append_block_from_url(
                sources_container_client.url + "/" + blob_name + "?" + sas_token,
                9 if header else None,
            )

    return {}
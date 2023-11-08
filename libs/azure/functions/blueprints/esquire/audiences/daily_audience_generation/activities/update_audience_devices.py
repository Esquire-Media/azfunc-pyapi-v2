#
from libs.azure.functions import Blueprint
from azure.storage.blob import (
    ContainerClient,
    ContainerSasPermissions,
    generate_container_sas,
)
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

bp: Blueprint = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_update_audience_devices(ingress: dict):
    # make connection to the container
    container_client = ContainerClient.from_connection_string(
        conn_str=os.environ["ONSPOT_CONN_STR"],
        container_name="general",
    )
    # generate sas token
    sas_token = generate_container_sas(
        account_name=container_client.account_name,
        account_key=container_client.credential.account_key,
        container_name=container_client.container_name,
        permission=ContainerSasPermissions(write=True, read=True),
        expiry=datetime.utcnow() + relativedelta(days=2),
    )
    destination_blob_client = container_client.get_blob_client(
        "audiences/" + ingress["audience_id"]
    )
    first = True

    for blob_name in container_client.list_blob_names(
        name_starts_with="{}/{}/audiences/{}/devices/".format(
            ingress["blob_prefix"], ingress["instance_id"], ingress["audience_id"]
        )
    ):
        if not blob_name.endswith(".debug.csv"):
            if first:
                destination_blob_client.create_append_blob()
                header = (
                    container_client.download_blob(blob_name, 0, 8).read()
                    == b"deviceid"
                )
                first = False

            destination_blob_client.append_block_from_url(
                container_client.url + "/" + blob_name + "?" + sas_token,
                9 if header else None,
            )

    return {}

# File: libs/azure/functions/blueprints/esquire/onspot/activities/merge_devices.py

from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    ContainerClient,
    ContainerSasPermissions,
    generate_blob_sas,
    generate_container_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
import os, logging

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
    headers = ",".join(ingress.get("header", ["deviceid"])).encode()
    header_size = len(headers)

    for blob_prop in sources_container_client.list_blobs(
        name_starts_with=ingress["source"]["blob_prefix"]
    ):
        if not blob_prop.name.endswith(".debug.csv"):
            if blob_prop.size > 9:
                if first:
                    destination_blob_client.create_append_blob()
                    first = False

                header = (
                    sources_container_client.download_blob(
                        blob_prop.name, 0, header_size
                    ).read()
                    == headers
                )

                url = (
                    sources_container_client.url
                    + "/"
                    + blob_prop.name
                    + "?"
                    + sas_token
                )

                logging.warning(url)

                destination_blob_client.append_block_from_url(
                    copy_source_url=url,
                    source_offset=header_size + 1 if header else None,
                )

    return destination_blob_client.url + "?" + generate_blob_sas(
        account_name=destination_blob_client.account_name,
        account_key=destination_blob_client.credential.account_key,
        container_name=destination_blob_client.container_name,
        blob_name=destination_blob_client.blob_name,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + relativedelta(days=2),
    )

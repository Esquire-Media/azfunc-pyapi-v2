#  file path:libs/azure/functions/blueprints/esquire/audiences/utils/activities/newestAudienceBlobPrefix.py

from azure.durable_functions import Blueprint
from azure.storage.blob import ContainerClient
import os

bp: Blueprint = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesUtils_newestAudienceBlobPrefix(ingress: dict):
    # ingress = {
    #     "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
    #     "container_name": "general",
    #     "audience_id": ingress,
    # }
    container_client = ContainerClient.from_connection_string(
        conn_str=os.environ.get(ingress["conn_str"], ingress["conn_str"]),
        container_name=ingress["container_name"],
    )
    most_recent_blob_prefix = None
    for blob in container_client.list_blobs(
        name_starts_with="audiences/{}/".format(ingress["audience_id"])
    ):
        path = "/".join(blob.name.split("/")[0:3])
        if not most_recent_blob_prefix or path > most_recent_blob_prefix:
            most_recent_blob_prefix = path

    return most_recent_blob_prefix

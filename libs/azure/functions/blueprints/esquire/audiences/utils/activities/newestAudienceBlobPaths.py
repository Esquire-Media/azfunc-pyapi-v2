#  file path:libs/azure/functions/blueprints/esquire/audiences/utils/activities/newestAudienceBlobPaths.py

from azure.storage.blob import ContainerClient
from libs.azure.functions import Blueprint
import os

bp: Blueprint = Blueprint()

@bp.activity_trigger(input_name="ingress")
async def activity_esquireAudiencesUtils_newestAudienceBlobPaths(ingress: dict):
    # ingress = {
    #     "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
    #     "container_name": "general",
    #     "audience_id": ingress,
    # }

    container_client = ContainerClient.from_connection_string(
        conn_str=os.environ.get(ingress["conn_str"], ingress["conn_str"]),
        container_name=ingress["container_name"]
    )
    most_recent_prefix = None
    most_recent_blobs = []

    for blob in container_client.list_blobs(
        name_starts_with="audiences/{}/".format(ingress["audience_id"])
    ):
        prefix = blob.name.split("/")[2]
        if not most_recent_prefix or prefix > most_recent_prefix:
            most_recent_prefix = prefix
            most_recent_blobs = []
        elif prefix == most_recent_prefix:
            most_recent_blobs.append(blob.name)

    return most_recent_blobs
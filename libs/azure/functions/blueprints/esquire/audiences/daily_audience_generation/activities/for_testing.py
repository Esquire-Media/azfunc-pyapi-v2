# File: libs/azure/functions/blueprints/esquire/audiences/daily_audience_generation/activities/for_testing.py

from libs.azure.functions import Blueprint
import logging
import os


bp: Blueprint = Blueprint()


# imports for testing only
from azure.storage.blob import ContainerClient


# activity to manipulate testing data
@bp.activity_trigger(input_name="ingress")
def activity_testing(ingress: dict):
    # move the test file into the correct location to simulate an automated file there
    container_client = ContainerClient.from_connection_string(
        conn_str=os.environ["ONSPOT_CONN_STR"],
        container_name="general",
    )
    source_blob_client = container_client.get_blob_client(
        "a0H6e00000bNazEEAS_test.csv"
    )
    destination_blob_client = container_client.get_blob_client(
        f"raw/{ingress['instance_id']}/audiences/a0H6e00000bNazEEAS_test/a0H6e00000bNazEEAS_test.csv"
    )
    destination_blob_client.start_copy_from_url(source_blob_client.url)

    return {}

# File: libs/azure/functions/blueprints/esquire/audiences/friends_family/test.py

from libs.azure.functions import Blueprint
from azure.durable_functions import (
    DurableOrchestrationClient,
)
from urllib.parse import unquote
from dateutil.relativedelta import relativedelta
from libs.azure.functions.http import HttpRequest
import os
from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime

bp: Blueprint = Blueprint()


# Define an HTTP-triggered function that starts a new orchestration
@bp.route(route="test/esquire/audiences/friends_family")
@bp.durable_client_input(client_name="client")
async def testTrigger_audience_friendsFamily(req: HttpRequest, client: DurableOrchestrationClient):
    # load the audience addresses file into a dataframe
    blob_client = BlobClient.from_connection_string(
        conn_str=os.environ["ONSPOT_CONN_STR"],
        container_name="general",
        blob_name="a0H6e00000bNazEEAS_test.csv",
    )

    instance_id = await client.start_new(
        "orchestrator_audience_friendsFamily",
        client_input={
            "source": (
                unquote(blob_client.url)
                + "?"
                + generate_blob_sas(
                    account_name=blob_client.account_name,
                    container_name=blob_client.container_name,
                    blob_name=blob_client.blob_name,
                    account_key=blob_client.credential.account_key,
                    permission=BlobSasPermissions(read=True),
                    expiry=datetime.utcnow() + relativedelta(days=2),
                )
            ),
            "destination": {
                "conn_str": "ONSPOT_CONN_STR",  # what if this isn't there -> default value
                "container_name": "general",
                "blob_name": "audiences/a0H6e00000bNazEEAS_test",
            },
        },
    )

    # Return a response that includes the status query URLs
    return client.create_check_status_response(req, instance_id)

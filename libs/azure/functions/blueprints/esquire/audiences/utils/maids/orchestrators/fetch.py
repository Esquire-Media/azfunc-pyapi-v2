# File: libs/azure/functions/blueprints/esquire/audiences/maids/orchestrators/fetch.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from dateutil.relativedelta import relativedelta
from datetime import datetime
from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.utils.maids.config import (
    maids_name,
    validated_addresses_name,
    geoframes_name,
)
from urllib.parse import unquote
import os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesMaids_fetch(context: DurableOrchestrationContext):
    """
    Orchestrator function to fetch and process audience data for Esquire Audiences Maids.

    This function handles various tasks such as fetching audience data, generating SAS tokens
    for blob storage, and orchestrating calls to sub-orchestrators for different audience types.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context for the orchestration, containing methods for interacting with the Durable Functions runtime.
        - audiences : list
            A list of audience data dictionaries, each containing the 'id' and 'type' of the audience.
        - destination : dict
            Details about the destination blob storage, including 'blob_prefix'.
        - source : dict
            Details about the source blob storage, including connection string, container name, and blob prefix.
        - working : dict
            Information about the working blob storage, including blob prefix.
        - execution_time : str
            The time of execution, used for generating blob names.
    """
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)
    execution_time = ingress["execution_time"]

    # Orchestrating tasks for different audience types
    tasks = []
    for audience in ingress["audiences"]:
        blob_client = BlobClient.from_connection_string(
            conn_str=os.environ[ingress["source"]["conn_str"]],
            container_name=ingress["source"]["container_name"],
            blob_name="{}/{}/{}/{}".format(
                ingress["source"]["blob_prefix"],
                audience["id"],
                execution_time,
                validated_addresses_name
                if audience["type"]
                in ["Past Customers", "New Movers", "Digital Neighbors"]
                else geoframes_name,
            ),
        )

        sas_token = generate_blob_sas(
            account_name=blob_client.account_name,
            container_name=blob_client.container_name,
            blob_name=blob_client.blob_name,
            account_key=blob_client.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )

        source_url = unquote(blob_client.url) + "?" + sas_token

        task_payload = {
            "working": {
                **ingress["working"],
                "outputPath": "{}/{}/devices".format(
                    ingress["working"]["blob_prefix"] + f"/{context.instance_id}"
                    if ingress["working"]["blob_prefix"]
                    else context.instance_id,
                    audience["id"],
                ),
            },
            "source": source_url,
            "destination": {
                **ingress["destination"],
                "blob_name": "{}/{}/{}/{}".format(
                    ingress["destination"]["blob_prefix"],
                    audience["id"],
                    execution_time,
                    maids_name,
                ),
            },
        }

        if audience["type"] in ["Past Customers", "New Movers", "Digital Neighbors"]:
            tasks.append(
                context.call_sub_orchestrator_with_retry(
                    "orchestrator_esquireAudienceMaidsAddresses_standard",
                    retry,
                    task_payload,
                )
            )
        elif audience["type"] in ["InMarket Shoppers", "Competitor Locations"]:
            tasks.append(
                context.call_sub_orchestrator_with_retry(
                    "orchestrator_esquireAudienceMaidsGeoframes_standard",
                    retry,
                    task_payload,
                )
            )
        elif audience["type"] == "Friends Family":
            tasks.append(
                context.call_sub_orchestrator_with_retry(
                    "orchestrator_esquireAudienceMaidsAddresses_footprint",
                    retry,
                    task_payload,
                )
            )

    # Executing all orchestrated tasks
    yield context.task_all(tasks)

    return {}

# File: libs/azure/functions/blueprints/esquire/audiences/maids/orchestrators/fetch.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from dateutil.relativedelta import relativedelta
from datetime import datetime
from libs.azure.functions.blueprints.esquire.audiences.utils.maids.config import (
    unvalidated_addresses_name,
    validated_addresses_name,
    geoframes_name,
)
from libs.utils.text import camel_case
from urllib.parse import unquote
import os

bp = Blueprint()

# main orchestrator
@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesMaids_prepare(context: DurableOrchestrationContext):
    """
    Main orchestrator function for preparing Esquire Audience Maids data.

    This function coordinates several tasks, such as fetching and validating addresses, 
    and preparing geoframes. It manages these tasks through Azure Durable Functions.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context object provided by the Azure Durable Function, which includes information 
        like input data and methods for calling sub-orchestrators and activities.
        - audiences : list
            A list of audience data dictionaries.
        - destination : dict
            Information about the destination for storing processed data.
        - source : dict
            Information about the data source.
        - execution_time : str, optional
            The time when the execution is started, defaults to the current UTC time.
        - fetch : bool, optional
            Flag to indicate whether to perform a fetch operation.
    """
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    # Setting the execution time
    execution_time = ingress.get(
        "execution_time", context.current_utc_datetime.isoformat()
    )

    # First task: Processing addresses for different audience types
    yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_esquireAudiencesMaidsAddresses_{}".format(
                    camel_case(audience["type"])
                ),
                retry,
                {
                    "audience": audience,
                    "destination": {
                        **ingress["destination"],
                        "blob_name": "{}/{}/{}/{}".format(
                            ingress["destination"]["blob_prefix"],
                            audience["id"],
                            execution_time,
                            unvalidated_addresses_name,
                        ),
                    },
                    "source": {
                        **ingress["source"],
                        "blob_name": "{}/{}/{}".format(
                            ingress["destination"]["blob_prefix"],
                            audience["id"],
                            unvalidated_addresses_name,
                        ),
                    }
                },
            )
            for audience in ingress["audiences"]
            if audience["type"]
            in [
                "Digital Neighbors",
                "Friends Family",
                "New Movers",
                "Past Customers",
            ]
        ]
    )

    # Second task: Validating addresses
    yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_smarty_validateAddresses",
                retry,
                {
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
                        **ingress["destination"],
                        "blob_name": "{}/{}/{}/{}".format(
                            ingress["destination"]["blob_prefix"],
                            audience["id"],
                            execution_time,
                            validated_addresses_name,
                        ),
                    },
                },
            )
            for audience in ingress["audiences"]
            if audience["type"]
            in [
                "Friends Family",
                "New Movers",
                "Digital Neighbors",
                "Past Customers",
            ]
            if (
                blob_client := BlobClient.from_connection_string(
                    conn_str=os.environ[ingress["source"]["conn_str"]],
                    container_name=ingress["source"]["container_name"],
                    blob_name="{}/{}/{}/{}".format(
                        ingress["source"]["blob_prefix"],
                        audience["id"],
                        execution_time,
                        unvalidated_addresses_name,
                    ),
                )
            )
        ]
    )

    # Third task: Preparing geoframes for specific audience types
    yield context.task_all(
        [
            context.call_activity_with_retry(
                "activity_esquireAudiencesMaidsGeoframes_geoframes",
                retry,
                {
                    "audience": audience,
                    "destination": {
                        **ingress["destination"],
                        "blob_name": "{}/{}/{}/{}".format(
                            ingress["destination"]["blob_prefix"],
                            audience["id"],
                            execution_time,
                            geoframes_name,
                        ),
                    },
                },
            )
            for audience in ingress["audiences"]
            if audience["type"] in ["InMarket Shoppers", "Competitor Locations"]
        ]
    )

    # Optional task: Fetching data if the 'fetch' flag is set
    if ingress.get("fetch", False):
        yield context.call_sub_orchestrator_with_retry(
            "orchestrator_esquireAudiencesMaids_fetch",
            retry,
            {**ingress, "execution_time": execution_time},
        )

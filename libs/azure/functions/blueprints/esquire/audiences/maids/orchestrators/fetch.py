# File: libs/azure/functions/blueprints/esquire/audiences/maids/orchestrators/fetch.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from dateutil.relativedelta import relativedelta
from datetime import datetime
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.maids.config import (
    maids_name,
    unvalidated_addresses_name,
    validated_addresses_name,
    geoframes_name
)
from urllib.parse import unquote
import os, logging

bp = Blueprint()


# main orchestrator
@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesMaids_fetch(context: DurableOrchestrationContext):
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    execution_time = ingress["execution_time"]

    yield context.task_all(
        [
            context.call_sub_orchestrator_with_retry(
                "orchestrator_esquireAudienceMaidsAddresses_standard",
                retry,
                {
                    "working": {
                        **ingress["working"],
                        "outputPath": "{}/{}/devices".format(
                            ingress["working"]["blob_prefix"]
                            + f"/{context.instance_id}"
                            if ingress["working"]["blob_prefix"]
                            else context.instance_id,
                            audience["id"],
                        ),
                    },
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
                            maids_name,
                        ),
                    },
                },
            )
            for audience in ingress["audiences"]
            if audience["type"]
            in [
                "Past Customers",
                "New Movers",
                "Digital Neighbors",
            ]
            if (
                blob_client := BlobClient.from_connection_string(
                    conn_str=os.environ[ingress["source"]["conn_str"]],
                    container_name=ingress["source"]["container_name"],
                    blob_name="{}/{}/{}/{}".format(
                        ingress["source"]["blob_prefix"],
                        audience["id"],
                        execution_time,
                        validated_addresses_name,
                    ),
                )
            )
        ] +
        [
            context.call_sub_orchestrator_with_retry(
                "orchestrator_esquireAudienceMaidsGeoframes_standard",
                retry,
                {
                    "working": {
                        **ingress["working"],
                        "outputPath": "{}/{}/devices".format(
                            ingress["working"]["blob_prefix"]
                            + f"/{context.instance_id}"
                            if ingress["working"]["blob_prefix"]
                            else context.instance_id,
                            audience["id"],
                        ),
                    },
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
                            maids_name,
                        ),
                    },
                },
            )
            for audience in ingress["audiences"]
            if audience["type"]
            in [
                "InMarket Shoppers",
                "Competitor Locations",
            ]
            if (
                blob_client := BlobClient.from_connection_string(
                    conn_str=os.environ[ingress["source"]["conn_str"]],
                    container_name=ingress["source"]["container_name"],
                    blob_name="{}/{}/{}/{}".format(
                        ingress["source"]["blob_prefix"],
                        audience["id"],
                        execution_time,
                        geoframes_name,
                    ),
                )
            )
        ]
        + [
            context.call_sub_orchestrator_with_retry(
                "orchestrator_esquireAudienceMaidsAddresses_footprint",
                retry,
                {
                    "working": {
                        **ingress["working"],
                        "outputPath": "{}/{}/devices".format(
                            ingress["working"]["blob_prefix"]
                            + f"/{context.instance_id}"
                            if ingress["working"]["blob_prefix"]
                            else context.instance_id,
                            audience["id"],
                        ),
                    },
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
                            maids_name,
                        ),
                    },
                },
            )
            for audience in ingress["audiences"]
            if audience["type"] == "Friends Family"
            if (
                blob_client := BlobClient.from_connection_string(
                    conn_str=os.environ[ingress["source"]["conn_str"]],
                    container_name=ingress["source"]["container_name"],
                    blob_name="{}/{}/{}/{}".format(
                        ingress["source"]["blob_prefix"],
                        audience["id"],
                        execution_time,
                        validated_addresses_name,
                    ),
                )
            )
        ]
    )

    return {}
#  file path:libs/azure/functions/blueprints/esquire/audiences/meta/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    BlobServiceClient,
    ContainerClient,
    ContainerSasPermissions,
    generate_blob_sas,
    generate_container_sas,
)
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
import os, datetime, logging
import pandas as pd
from urllib.parse import unquote

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def meta_customaudience_orchestrator(
    context: DurableOrchestrationContext,
):
    # determine the blob path to latest maids.csv file
    audience_id = context.get_input()
    source = {
        "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
        "container_name": "general",
        "blob_prefix": f"audiences/{audience_id}",
    }
    
    # get the latest audience file
    latest_blob = yield context.call_activity(
        'activity_esquireAudiencesMeta_newestAudience',
        source
    )
    
    # get count of devices in this file
    source_blob_client = BlobClient.from_connection_string(
        conn_str=os.environ["ONSPOT_CONN_STR"],
        container_name="general",
        blob_name=most_recent_file,
    )

    source_url = (
        unquote(source_blob_client.url)
        + "?"
        + generate_blob_sas(
            account_name=source_blob_client.account_name,
            container_name=source_blob_client.container_name,
            blob_name=source_blob_client.blob_name,
            account_key=source_blob_client.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
    )

    count = pd.read_csv(source_url, header=None).count().iloc[0]

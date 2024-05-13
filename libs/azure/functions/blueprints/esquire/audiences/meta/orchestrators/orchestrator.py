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
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from libs.azure.functions import Blueprint
import os, datetime, logging, json
from io import BytesIO
import pandas as pd
from urllib.parse import unquote

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def meta_customaudience_orchestrator(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    # ingress={
    #     "meta":{
    #         "adaccountid":"act_947970545616788",
    #         "audienceid":"120208598420700391",
    #     },
    #     "esq":{
    #         "audienceid":"clulpbfdg001v12jixniohdne", #not a venue replay
    #     }
    # },

    newAudienceNeeded = not ingress["meta"]["audienceid"]

    if not newAudienceNeeded:
        metaAudience = yield context.call_sub_orchestrator(
            "meta_orchestrator_request",
            {
                "operationId": "CustomAudience.Get",
                "parameters": {
                    "CustomAudience-Id": ingress["meta"]["audienceid"],
                },
            },
        )
        newAudienceNeeded = bool(metaAudience.get("error", False))

    # if there is no facebook audience ID
    if newAudienceNeeded:
        # create a new audience in facebook
        context.set_custom_status("Creating new Meta Audience.")
        metaAudience = yield context.call_sub_orchestrator(
            "meta_orchestrator_request",
            {
                "operationId": "AdAccount.Post.Customaudiences",
                "parameters": {
                    "AdAccount-Id": ingress["meta"]["adaccountid"],
                    "name": "testName",
                    "description": ingress["esq"]["audienceid"],
                    "customer_file_source": "USER_PROVIDED_ONLY",
                    "subtype": "CUSTOM",
                },
            },
        )

    # get list of the device IDs
    # logging.warning(metaAudience)

    # pass the information to get device IDs and to
    yield context.call_activity(
        "activity_esquireAudiencesMeta_facebookUpdateAudience",
        {
            "conn_str":os.environ["ESQUIRE_AUDIENCE_CONN_STR"],
            "container_name":"general",
            "blob_name":"audiences/a0H5A00000aZbI1UAK/2023-12-04T18:16:59.757249+00:00/maids.csv",
            "audience_id":ingress["esq"]["audienceid"],
        },
    )

    return metaAudience

    # # get count of devices in this file
    # source_blob_client = BlobClient.from_connection_string(
    #     conn_str=os.environ["ONSPOT_CONN_STR"],
    #     container_name="general",
    #     blob_name=latest_blob,
    # )

    # source_url = (
    #     unquote(source_blob_client.url)
    #     + "?"
    #     + generate_blob_sas(
    #         account_name=source_blob_client.account_name,
    #         container_name=source_blob_client.container_name,
    #         blob_name=source_blob_client.blob_name,
    #         account_key=source_blob_client.credential.account_key,
    #         permission=BlobSasPermissions(read=True),
    #         expiry=datetime.utcnow() + relativedelta(days=2),
    #     )
    # )

    # count = pd.read_csv(source_url, header=None).count().iloc[0]

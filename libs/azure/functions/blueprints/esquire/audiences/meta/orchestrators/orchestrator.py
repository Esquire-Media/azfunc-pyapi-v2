#  file path:libs/azure/functions/blueprints/esquire/audiences/meta/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from azure.storage.blob import (
    BlobClient,
)
from libs.azure.functions import Blueprint
import os, json, random
from io import BytesIO
import pandas as pd
import hashlib


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

    # get the device IDs (static for testing)
    blob = BlobClient.from_connection_string(
        conn_str=os.environ["ESQUIRE_AUDIENCE_CONN_STR"],
        container_name="general",
        blob_name="audiences/a0H5A00000aZbI1UAK/2023-12-04T18:16:59.757249+00:00/maids.csv",
    )
    df = pd.read_csv(BytesIO(blob.download_blob().readall()), header=None)
    df.columns = ["DeviceID"]

    short_maid_list = df["DeviceID"].head(100).tolist()
    total_devices = 100

    # adding users
    context.set_custom_status("Creating adding users to Meta Audience.")
    session_id = random.randint(0, 2**32 - 1)
    metaAudience = yield context.call_sub_orchestrator(
        "meta_orchestrator_request",
        {
            "operationId": "CustomAudience.Post.Usersreplace",
            "parameters": {
                "CustomAudience-Id": ingress["meta"]["audienceid"],
                "payload": json.dumps(
                    {
                        "schema": "MOBILE_ADVERTISER_ID",
                        "data_source": {
                            "type": "THIRD_PARTY_IMPORTED",
                            "sub_type": "MOBILE_ADVERTISER_IDS",
                        },
                        "is_raw": True,
                        "data": short_maid_list,
                    }
                ),
                "session": json.dumps(
                    {
                        "session_id": session_id,
                        "estimated_num_total": total_devices,
                        "batch_seq": 1,
                        "last_batch_flag": True,
                    }
                ),
            },
        },
    )

    # pass the information to get device IDs and push users to Meta - works?
    # yield context.call_activity(
    #     "activity_esquireAudiencesMeta_facebookUpdateAudience",
    #     {
    #         "conn_str": os.environ["ESQUIRE_AUDIENCE_CONN_STR"],
    #         "container_name": "general",
    #         "blob_name": "audiences/a0H5A00000aZbI1UAK/2023-12-04T18:16:59.757249+00:00/maids.csv",
    #         "audience_id": ingress["esq"]["audienceid"],
    #     },
    # )

    return metaAudience


# function to has the MAID values passed into Facebook audience
def hash_maid(maid):
    return hashlib.sha256(maid.encode("utf-8")).hexdigest()

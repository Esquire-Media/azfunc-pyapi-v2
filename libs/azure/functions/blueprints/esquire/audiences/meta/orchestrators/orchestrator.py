#  file path:libs/azure/functions/blueprints/esquire/audiences/meta/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
import os, json, random, re, logging, datetime
import pandas as pd


bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def meta_customaudience_orchestrator(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    # ingress="clulpbfdg001v12jixniohdne"

    # reach out to audience definition DB - get adaccount and audienceid (if it exists)
    # ids = yield context.call_activity(
    #     "activity_esquireAudienceMeta_fetchAudience",
    #     ingress,
    # )

    # newAudienceNeeded = not ids["audience"]

    # if not newAudienceNeeded:
    #     metaAudience = yield context.call_sub_orchestrator(
    #         "meta_orchestrator_request",
    #         {
    #             "operationId": "CustomAudience.Get",
    #             "parameters": {
    #                 "CustomAudience-Id": ids["audience"],
    #             },
    #         },
    #     )
    #     newAudienceNeeded = bool(metaAudience.get("error", False))

    # # if there is no facebook audience ID
    # if newAudienceNeeded:
    #     # create a new audience in facebook
    #     context.set_custom_status("Creating new Meta Audience.")
    #     metaAudience = yield context.call_sub_orchestrator(
    #         "meta_orchestrator_request",
    #         {
    #             "operationId": "AdAccount.Post.Customaudiences",
    #             "parameters": {
    #                 "AdAccount-Id": ids["adAccount"],
    #                 "name": "testName",  # this needs to be dynamically generated, static until this is possible.
    #                 "description": ingress,
    #                 "customer_file_source": "USER_PROVIDED_ONLY",
    #                 "subtype": "CUSTOM",
    #             },
    #         },
    #     )
    #     # need to update the database to have that new audience ID - there is no column in DB for this at this time

    # activity to get the folder with the most recent MAIDs
    blobs_path = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudience",
        {
            "conn_str": os.environ["ESQUIRE_AUDIENCE_CONN_STR"],
            "container_name": "general",
            "audience_id": ingress,
        },
    )

    # get object with all of the blob URLs and how many MAIDs are in that file
    url_maids = yield context.call_activity(
        "activity_esquireAudiencesUtils_getTotalMaids",
        {
            "conn_str": os.environ["ESQUIRE_AUDIENCE_CONN_STR"],
            "container_name": "general",
            "path_to_blobs": blobs_path,
            "audience_id": ingress,
        },
    )

    maids_info, blob_count = url_maids

    # Create the list of tasks
    context.set_custom_status("Creating list of user tasks for Meta Audience.")
    session_id = random.randint(0, 2**32 - 1)

    metaAudience = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "meta_orchestrator_request",
                {
                    "operationId": "CustomAudience.Post.Usersreplace",
                    "parameters": {
                        # "CustomAudience-Id": ids["audience"],
                        "CustomAudience-Id": "23854129551430390",
                        "payload": json.dumps(
                            {
                                "schema": "MOBILE_ADVERTISER_ID",
                                "data_source": {
                                    "type": "THIRD_PARTY_IMPORTED",
                                    "sub_type": "MOBILE_ADVERTISER_IDS",
                                },
                                "is_raw": True,
                                "data": BlobClient.from_blob_url(value["url"]).download_blob().readall().decode('utf-8').split('\r\n')[1:-1],
                            }
                        ),
                        "session": json.dumps(
                            {
                                "session_id": session_id,
                                "estimated_num_total": value["maids_count"],
                                "batch_seq": int(
                                    re.search(r"\d+", key).group()
                                ),  # Use the current blob number for batch sequence
                                "last_batch_flag": (
                                    True
                                    if int(re.search(r"\d+", key).group()) == blob_count
                                    else False
                                ), #True is the the blob number is equal to the total number of blobs
                            }
                        ),
                    },
                },
            )
            for key, value in maids_info.items()
            if key.startswith("Blob_")
        ]
    )
    logging.warning(metaAudience)
    
    # # run the task_all for the created lsit of tasks to add all the users
    # context.set_custom_status("Adding users to Meta Audience.")
    # metaAudience = yield context.task_all(task_list)
    
    return {}
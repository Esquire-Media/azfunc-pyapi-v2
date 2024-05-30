#  file path:libs/azure/functions/blueprints/esquire/audiences/meta/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
from datetime import datetime
import os, json, random, re, logging
import pandas as pd


bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def meta_customaudience_orchestrator(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    # ingress="clwjn2qeu005drw043l2lrnbv"

    # reach out to audience definition DB - get adaccount and audienceid (if it exists)
    ids = yield context.call_activity(
        "activity_esquireAudienceMeta_fetchAudience",
        ingress,
    )

    newAudienceNeeded = not ids["audience"]

    if not newAudienceNeeded:
        logging.warning("Do not need to create an audience.")
        metaAudience = yield context.call_sub_orchestrator(
            "meta_orchestrator_request",
            {
                "operationId": "CustomAudience.Get",
                "parameters": {
                    "CustomAudience-Id": ids["audience"],
                    "fields": ["operation_status"],
                },
            },
        )
        newAudienceNeeded = bool(metaAudience.get("error", False))

        # get the status of the audience - if running, cancel it
        logging.warning(("Line 44: ", metaAudience))
        match metaAudience["operation_status"]["code"]:
            # Replace in progress
            case 300 | 414:
                metaAudience = yield context.call_sub_orchestrator(
                    "meta_orchestrator_request",
                    {
                        "operationId": "CustomAudience.Get",
                        "parameters": {
                            "CustomAudience-Id": ids["audience"],
                            "fields": ["operation_status"],
                        },
                        "session": json.dumps(
                            {
                                "last_batch_flag": True,
                            }
                        ),
                    },
                )
                # throw exception 
                # logging.warning(("Line 63: ", metaAudience))
    # return {}
    # if there is no facebook audience ID
    if newAudienceNeeded:
        # create a new audience in facebook
        logging.warning("Creating an audience.")
        context.set_custom_status("Creating new Meta Audience.")
        metaAudience = yield context.call_sub_orchestrator(
            "meta_orchestrator_request",
            {
                "operationId": "AdAccount.Post.Customaudiences",
                "parameters": {
                    "AdAccount-Id": ids["adAccount"],
                    "name": f"{'_'.join(ids['tags'])}_{ingress}",
                    "description": ingress,
                    "customer_file_source": "USER_PROVIDED_ONLY",
                    "subtype": "CUSTOM",
                },
            },
        )
        # need to update the database to have that new audience ID - there is no column in DB for this at this time
        yield context.call_activity(
            "activity_esquireAudienceMeta_putAudience",
            {
                "audience": ingress,
                "metaAudienceId": metaAudience["id"],
            },
        )

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
    # url_maids = yield context.call_activity(
    #     "activity_esquireAudiencesUtils_getTotalMaids",
    #     {
    #         "conn_str": os.environ["ESQUIRE_AUDIENCE_CONN_STR"],
    #         "container_name": "general",
    #         "path_to_blobs": blobs_path,
    #         "audience_id": ingress,
    #     },
    # )

    # logging.warning(url_maids)

    # get list of all maids
    maids_info = yield context.call_activity(
        "activity_esquireAudiencesUtils_getMaids",
        {
            "conn_str": os.environ["ESQUIRE_AUDIENCE_CONN_STR"],
            "container_name": "general",
            "path_to_blobs": blobs_path,
        },
    )
    
    # maids, count = maids_info
    # logging.warning(("Line 126: ", len(maids)))
    # logging.warning(("Line 127: ", count))
    
    return {}
    # Add the users
    context.set_custom_status("Adding users to Meta Audience.")

    sessionStarter = yield context.call_sub_orchestrator(
        "meta_orchestrator_request",
        {
            "operationId": "CustomAudience.Post.Usersreplace",
            "parameters": {
                "CustomAudience-Id": metaAudience["id"],
                "payload": json.dumps(
                    {
                        "schema": "MOBILE_ADVERTISER_ID",
                        "data_source": {
                            "type": "THIRD_PARTY_IMPORTED",
                            "sub_type": "MOBILE_ADVERTISER_IDS",
                        },
                        "is_raw": True,
                        "data": maids[0]
                    }
                ),
                "session": json.dumps(
                    {
                        "session_id": random.randint(0, 2**32 - 1),
                        "estimated_num_total": count,
                        "batch_seq": 1,
                        "last_batch_flag": 1 == len(maids),
                    }
                ),
            },
        },
    )
    logging.warning(("Line 147: ", sessionStarter))

    metaAudience_list = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "meta_orchestrator_request",
                {
                    "operationId": "CustomAudience.Post.Usersreplace",
                    "parameters": {
                        "CustomAudience-Id": metaAudience["id"],
                        "payload": json.dumps(
                            {
                                "schema": "MOBILE_ADVERTISER_ID",
                                "data_source": {
                                    "type": "THIRD_PARTY_IMPORTED",
                                    "sub_type": "MOBILE_ADVERTISER_IDS",
                                },
                                "is_raw": True,
                                "data": maid_list,
                            }
                        ),
                        "session": json.dumps(
                            {
                                "session_id": sessionStarter["session_id"],
                                "estimated_num_total": count,
                                "batch_seq": index + 1,
                                "last_batch_flag": index + 1 == len(maids),
                            }
                        ),
                    },
                },
            )
            for index, maid_list in enumerate(maids, 1)
        ]
    )
    logging.warning(("Line 187: ", metaAudience_list))

    return {}
    # Add the users
    context.set_custom_status("Adding users to Meta Audience.")

    sessionStarter = yield context.call_sub_orchestrator(
        "meta_orchestrator_request",
        {
            "operationId": "CustomAudience.Post.Usersreplace",
            "parameters": {
                "CustomAudience-Id": metaAudience["id"],
                "payload": json.dumps(
                    {
                        "schema": "MOBILE_ADVERTISER_ID",
                        "data_source": {
                            "type": "THIRD_PARTY_IMPORTED",
                            "sub_type": "MOBILE_ADVERTISER_IDS",
                        },
                        "is_raw": True,
                        "data": BlobClient.from_blob_url(url_maids[0]["url"])
                        .download_blob()
                        .readall()
                        .decode("utf-8")
                        .split("\r\n")[1:-1],
                    }
                ),
                "session": json.dumps(
                    {
                        "session_id": random.randint(0, 2**32 - 1),
                        "estimated_num_total": url_maids[0]["count"],
                        "batch_seq": 1,
                        "last_batch_flag": 1 == len(url_maids),
                    }
                ),
            },
        },
    )
    logging.warning(("Line 147: ", sessionStarter))

    metaAudience_list = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "meta_orchestrator_request",
                {
                    "operationId": "CustomAudience.Post.Usersreplace",
                    "parameters": {
                        "CustomAudience-Id": metaAudience["id"],
                        "payload": json.dumps(
                            {
                                "schema": "MOBILE_ADVERTISER_ID",
                                "data_source": {
                                    "type": "THIRD_PARTY_IMPORTED",
                                    "sub_type": "MOBILE_ADVERTISER_IDS",
                                },
                                "is_raw": True,
                                "data": BlobClient.from_blob_url(blob["url"])
                                .download_blob()
                                .readall()
                                .decode("utf-8")
                                .split("\r\n")[1:-1],
                            }
                        ),
                        "session": json.dumps(
                            {
                                "session_id": sessionStarter["session_id"],
                                "estimated_num_total": blob["count"],
                                "batch_seq": index + 1,
                                "last_batch_flag": index + 1 == len(url_maids),
                            }
                        ),
                    },
                },
            )
            for index, blob in enumerate(url_maids, 1)
        ]
    )
    logging.warning(("Line 187: ", metaAudience_list))

    return metaAudience

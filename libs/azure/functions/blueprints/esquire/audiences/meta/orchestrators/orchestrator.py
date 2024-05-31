#  file path:libs/azure/functions/blueprints/esquire/audiences/meta/orchestrator.py
from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
import os, json, uuid, logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def meta_customaudience_orchestrator(
    context: DurableOrchestrationContext,
):
    batch_size = 10000
    audience_id = context.get_input()
    # audience_id="clwjn2qeu005drw043l2lrnbv"

    # reach out to audience definition DB - get adaccount and audienceid (if it exists)
    ids = yield context.call_activity(
        "activity_esquireAudienceMeta_fetchAudience",
        audience_id,
    )

    newAudienceNeeded = not ids["audience"]

    if not newAudienceNeeded:
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

    if newAudienceNeeded:
        # create a new audience in facebook
        context.set_custom_status("Creating new Meta Audience.")
        metaAudience = yield context.call_sub_orchestrator(
            "meta_orchestrator_request",
            {
                "operationId": "AdAccount.Post.Customaudiences",
                "parameters": {
                    "AdAccount-Id": ids["adAccount"],
                    "name": f"{'_'.join(ids['tags'])}_{audience_id}",
                    "description": audience_id,
                    "customer_file_source": "USER_PROVIDED_ONLY",
                    "subtype": "CUSTOM",
                },
            },
        )
        # need to update the database to have that new audience ID - there is no column in DB for this at this time
        yield context.call_activity(
            "activity_esquireAudienceMeta_putAudience",
            {
                "audience": audience_id,
                "metaAudienceId": metaAudience["id"],
            },
        )

    if metaAudience.get("operation_status", False):
        # get the status of the audience
        match metaAudience["operation_status"]["code"]:
            # Update in progress
            case 300 | 414:
                # Hacky way to close out a stuck session
                sessions = yield context.call_sub_orchestrator(
                    "meta_orchestrator_request",
                    {
                        "operationId": "CustomAudience.Get.Sessions",
                        "parameters": {"CustomAudience-Id": ids["audience"]},
                    },
                )
                for s in sessions:
                    if s["stage"] in ["uploading"]:
                        yield context.call_sub_orchestrator(
                            "meta_orchestrator_request",
                            {
                                "operationId": "CustomAudience.Post.Usersreplace",
                                "payload": json.dumps(
                                    {
                                        "schema": "MOBILE_ADVERTISER_ID",
                                        "data_source": {
                                            "type": "THIRD_PARTY_IMPORTED",
                                            "sub_type": "MOBILE_ADVERTISER_IDS",
                                        },
                                        "is_raw": True,
                                        "data": [str(uuid.uuid4())],
                                    }
                                ),
                                "session": json.dumps(
                                    {
                                        "session_id": s["session_id"],
                                        "estimated_num_total": int(s["num_received"])
                                        + 1,
                                        "batch_seq": int(s["num_received"])
                                        // batch_size
                                        + 1,
                                        "last_batch_flag": True,
                                    }
                                ),
                            },
                        )

    # activity to get the folder with the most recent MAIDs
    audience_blob_prefix = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPrefix",
        {
            "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
            "container_name": os.environ["ESQUIRE_AUDIENCE_CONTAINER_NAME"],
            "audience_id": audience_id,
        },
    )

    response = yield context.call_activity(
        "activity_synapse_query",
        {
            "bind": "audiences",
            "query": """
                SELECT 
                    COUNT(DISTINCT deviceid) AS [count]
                FROM OPENROWSET(
                    BULK '{}/{}',
                    DATA_SOURCE = '{}',  
                    FORMAT = 'CSV',
                    PARSER_VERSION = '2.0',
                    HEADER_ROW = TRUE
                ) AS [data]""".format(
                os.environ["ESQUIRE_AUDIENCE_CONTAINER_NAME"],
                audience_blob_prefix,
                os.environ["ESQUIRE_AUDIENCE_DATA_SOURCE"],
            ),
        },
    )
    count = response[0]["count"]
    
    results = yield context.task_all(
        [
            context.call_activity(
                "activity_synapse_query",
                {
                    "bind": "audiences",
                    "query": """
                    SELECT DISTINCT deviceid
                    FROM OPENROWSET(
                        BULK '{}/{}',
                        DATA_SOURCE = '{}',  
                        FORMAT = 'CSV',
                        PARSER_VERSION = '2.0',
                        HEADER_ROW = TRUE
                    ) WITH (
                        deviceid UNIQUEIDENTIFIER
                    ) AS [data]
                    ORDER BY deviceid
                    OFFSET {} ROWS
                    FETCH NEXT {} ROWS ONLY
                    """.format(
                        os.environ["ESQUIRE_AUDIENCE_CONTAINER_NAME"],
                        audience_blob_prefix,
                        os.environ["ESQUIRE_AUDIENCE_DATA_SOURCE"],
                        offset,
                        batch_size,
                    ),
                },
            )
            for offset in range(0, count, batch_size)
        ]
    )
    return results

    # # Add the users
    # context.set_custom_status("Adding users to Meta Audience.")

    # sessionStarter = yield context.call_sub_orchestrator(
    #     "meta_orchestrator_request",
    #     {
    #         "operationId": "CustomAudience.Post.Usersreplace",
    #         "parameters": {
    #             "CustomAudience-Id": metaAudience["id"],
    #             "payload": json.dumps(
    #                 {
    #                     "schema": "MOBILE_ADVERTISER_ID",
    #                     "data_source": {
    #                         "type": "THIRD_PARTY_IMPORTED",
    #                         "sub_type": "MOBILE_ADVERTISER_IDS",
    #                     },
    #                     "is_raw": True,
    #                     "data": maids[0]
    #                 }
    #             ),
    #             "session": json.dumps(
    #                 {
    #                     "session_id": random.randint(0, 2**32 - 1),
    #                     "estimated_num_total": count,
    #                     "batch_seq": 1,
    #                     "last_batch_flag": 1 == len(maids),
    #                 }
    #             ),
    #         },
    #     },
    # )

    # metaAudience_list = yield context.task_all(
    #     [
    #         context.call_sub_orchestrator(
    #             "meta_orchestrator_request",
    #             {
    #                 "operationId": "CustomAudience.Post.Usersreplace",
    #                 "parameters": {
    #                     "CustomAudience-Id": metaAudience["id"],
    #                     "payload": json.dumps(
    #                         {
    #                             "schema": "MOBILE_ADVERTISER_ID",
    #                             "data_source": {
    #                                 "type": "THIRD_PARTY_IMPORTED",
    #                                 "sub_type": "MOBILE_ADVERTISER_IDS",
    #                             },
    #                             "is_raw": True,
    #                             "data": maid_list,
    #                         }
    #                     ),
    #                     "session": json.dumps(
    #                         {
    #                             "session_id": sessionStarter["session_id"],
    #                             "estimated_num_total": count,
    #                             "batch_seq": index + 1,
    #                             "last_batch_flag": index + 1 == len(maids),
    #                         }
    #                     ),
    #                 },
    #             },
    #         )
    #         for index, maid_list in enumerate(maids, 1)
    #     ]
    # )

    # return {}
    # # Add the users
    # context.set_custom_status("Adding users to Meta Audience.")

    # sessionStarter = yield context.call_sub_orchestrator(
    #     "meta_orchestrator_request",
    #     {
    #         "operationId": "CustomAudience.Post.Usersreplace",
    #         "parameters": {
    #             "CustomAudience-Id": metaAudience["id"],
    #             "payload": json.dumps(
    #                 {
    #                     "schema": "MOBILE_ADVERTISER_ID",
    #                     "data_source": {
    #                         "type": "THIRD_PARTY_IMPORTED",
    #                         "sub_type": "MOBILE_ADVERTISER_IDS",
    #                     },
    #                     "is_raw": True,
    #                     "data": BlobClient.from_blob_url(url_maids[0]["url"])
    #                     .download_blob()
    #                     .readall()
    #                     .decode("utf-8")
    #                     .split("\r\n")[1:-1],
    #                 }
    #             ),
    #             "session": json.dumps(
    #                 {
    #                     "session_id": random.randint(0, 2**32 - 1),
    #                     "estimated_num_total": url_maids[0]["count"],
    #                     "batch_seq": 1,
    #                     "last_batch_flag": 1 == len(url_maids),
    #                 }
    #             ),
    #         },
    #     },
    # )

    # metaAudience_list = yield context.task_all(
    #     [
    #         context.call_sub_orchestrator(
    #             "meta_orchestrator_request",
    #             {
    #                 "operationId": "CustomAudience.Post.Usersreplace",
    #                 "parameters": {
    #                     "CustomAudience-Id": metaAudience["id"],
    #                     "payload": json.dumps(
    #                         {
    #                             "schema": "MOBILE_ADVERTISER_ID",
    #                             "data_source": {
    #                                 "type": "THIRD_PARTY_IMPORTED",
    #                                 "sub_type": "MOBILE_ADVERTISER_IDS",
    #                             },
    #                             "is_raw": True,
    #                             "data": BlobClient.from_blob_url(blob["url"])
    #                             .download_blob()
    #                             .readall()
    #                             .decode("utf-8")
    #                             .split("\r\n")[1:-1],
    #                         }
    #                     ),
    #                     "session": json.dumps(
    #                         {
    #                             "session_id": sessionStarter["session_id"],
    #                             "estimated_num_total": blob["count"],
    #                             "batch_seq": index + 1,
    #                             "last_batch_flag": index + 1 == len(url_maids),
    #                         }
    #                     ),
    #                 },
    #             },
    #         )
    #         for index, blob in enumerate(url_maids, 1)
    #     ]
    # )

    # return metaAudience

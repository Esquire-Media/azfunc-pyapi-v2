# File path: libs/azure/functions/blueprints/esquire/audiences/meta/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions import Blueprint
from libs.data import from_bind
import os, uuid, random, pandas as pd, orjson as json

# Initialize a Blueprint object to define and manage functions
bp = Blueprint()


# Define the orchestration trigger function for managing Meta custom audiences
@bp.orchestration_trigger(context_name="context")
def meta_customaudience_orchestrator(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the process of managing and updating Meta custom audiences.

    This function handles the creation of new Meta audiences, fetching existing audience information,
    updating audience data, and adding users to the audience.

    Args:
        context (DurableOrchestrationContext): The orchestration context.
    """
    batch_size = 1000  # Define the batch size for processing audience data
    audience_id = context.get_input()  # Get the audience ID from the input

    # Fetch audience definition from the database
    ids = yield context.call_activity(
        "activity_esquireAudienceMeta_fetchAudience",
        audience_id,
    )

    # Determine if a new Meta audience needs to be created
    newAudienceNeeded = not ids["audience"]

    if not newAudienceNeeded:
        # Check the status of the existing Meta audience
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
        # If there is an error, a new audience is needed
        newAudienceNeeded = bool(metaAudience.get("error", False))

    if newAudienceNeeded:
        # Create a new Meta audience
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
        # Update the database with the new audience ID
        yield context.call_activity(
            "activity_esquireAudienceMeta_putAudience",
            {
                "audience": audience_id,
                "metaAudienceId": metaAudience["id"],
            },
        )

    if metaAudience.get("operation_status", False):
        # Handle the status of the audience
        match metaAudience["operation_status"]["code"]:
            case 300 | 414:  # Update in progress
                # Close out a stuck session if any
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
                                ).decode(),
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
                                ).decode(),
                            },
                        )

    # Get the folder with the most recent MAIDs (Mobile Advertiser IDs)
    audience_blob_prefix = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPrefix",
        {
            "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
            "container_name": os.environ["ESQUIRE_AUDIENCE_CONTAINER_NAME"],
            "audience_id": audience_id,
        },
    )

    # Query to count distinct device IDs in the audience data
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

    # Add users to the Meta audience
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
                        "data": pd.read_sql(
                            """
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
                                0,
                                batch_size,
                            ),
                            from_bind("audiences").connect().connection(),
                        )["deviceid"]
                        .apply(lambda x: str(x))
                        .to_list(),
                    }
                ).decode("utf-8"),
                "session": json.dumps(
                    {
                        "session_id": random.randint(0, 2**32 - 1),
                        "estimated_num_total": count,
                        "batch_seq": 1,
                        "last_batch_flag": count < batch_size,
                    }
                ).decode("utf-8"),
            },
        },
    )
    sessionActions = [sessionStarter]

    # Handle batching if count exceeds batch_size
    if count > batch_size:
        for batch_seq, offset in enumerate(range(batch_size, count, batch_size), 2):
            sessionAction = yield context.call_sub_orchestrator(
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
                                "data": pd.read_sql(
                                    """
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
                                    from_bind("audiences").connect().connection(),
                                )["deviceid"]
                                .apply(lambda x: str(x))
                                .to_list(),
                            }
                        ).decode("utf-8"),
                        "session": json.dumps(
                            {
                                "session_id": sessionStarter["session_id"],
                                "estimated_num_total": count,
                                "batch_seq": batch_seq,
                                "last_batch_flag": count < offset + batch_size,
                            }
                        ).decode("utf-8"),
                    },
                },
            )
            sessionActions.append(sessionAction)

    return sessionActions  # Return the list of session actions to be executed

# File path: libs/azure/functions/blueprints/esquire/audiences/meta/orchestrator.py

from datetime import timedelta
from azure.durable_functions import Blueprint, DurableOrchestrationContext

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
    batch_size = 10000  # Define the batch size for processing audience data
    ingress = context.get_input()  # Get the audience ID from the input

    # Fetch audience definition from the database
    try:
        audience = yield context.call_activity(
            "activity_esquireAudienceMeta_fetchAudience",
            ingress["audience"]["id"],
        )
        ingress["audience"].update(audience)
        ingress["audience"]["name"] = (
            " - ".join(ingress["audience"]["tags"])
            if len(ingress["audience"]["tags"])
            else ingress["audience"]["id"]
        )
    except:
        return {}

    # Get or create the custom audience on Meta
    if not ingress["audience"]["audience"]:
        custom_audience = yield context.call_activity(
            "activity_esquireAudienceMeta_customAudience_create",
            ingress,
        )
        # Store the new audience ID in the database
        yield context.call_activity(
            "activity_esquireAudienceMeta_putAudience",
            {
                "audience": ingress["audience"]["id"],
                "metaAudienceId": custom_audience["id"],
            },
        )
    else:
        custom_audience = yield context.call_activity(
            "activity_esquireAudienceMeta_customAudience_get",
            ingress,
        )

    # Update the audience name and description if they differ
    if (
        custom_audience["name"] != ingress["audience"]["name"]
        or custom_audience["description"] != ingress["audience"]["id"]
    ):
        custom_audience = yield context.call_activity(
            "activity_esquireAudienceMeta_customAudience_update",
            ingress,
        )

    if custom_audience.get("operation_status", False):
        # Handle the status of the audience
        match custom_audience["operation_status"]["code"]:
            case 300 | 414:  # Update in progress
                # Close out a stuck session if any
                sessions = yield context.call_activity(
                    "activity_esquireAudienceMeta_customAudienceSessions_get",
                    ingress,
                )
                for s in sessions:
                    if s["stage"] in ["uploading"]:
                        yield context.call_activity(
                            "activity_esquireAudienceMeta_customAudienceSession_forceEnd",
                            {
                                **ingress,
                                "batch": {
                                    "session_id": s["session_id"],
                                    "total": int(s["num_received"]) + 1,
                                    "sequence": int(s["num_received"]) // batch_size
                                    + 1,
                                },
                            },
                        )
    ingress["audience"]["audience"] = custom_audience["id"]

    # Get the folder with the most recent MAIDs (Mobile Advertiser IDs)
    ingress["destination"]["blob_prefix"] = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPrefix",
        {
            "conn_str": ingress["destination"]["conn_str"],
            "container_name": ingress["destination"]["container_name"],
            "audience_id": ingress["audience"]["id"],
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
                    BULK '{}/{}/*',
                    DATA_SOURCE = '{}',  
                    FORMAT = 'CSV',
                    PARSER_VERSION = '2.0',
                    HEADER_ROW = TRUE
                ) AS [data]""".format(
                ingress["destination"]["container_name"],
                ingress["destination"]["blob_prefix"],
                ingress["destination"]["data_source"],
            ),
        },
    )

    # Add users to the Meta audience
    session = {}
    for sequence, offset in enumerate(range(0, response[0]["count"], batch_size)):
        while True:
            context.set_custom_status("Adding users to Meta Audience.")
            session = yield context.call_activity(
                "activity_esquireAudienceMeta_customAudience_replaceUsers",
                {
                    **ingress,
                    "sql": {
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
                                deviceid VARCHAR(36)
                            ) AS [data]
                        """,
                    },
                    "batch": {
                        "session": session,
                        "sequence": sequence,
                        "size": batch_size,
                        "total": response[0]["count"],
                    },
                },
            )
            if session.get("error", False):
                # Handle specific error codes by waiting and retrying if the audience is being updated
                if session["error"].get("code") == 2650 and (
                    session["error"].get("error_subcode") == 1870145
                    or session["error"].get("error_subcode") == 1870158
                ):
                    context.set_custom_status(
                        "Waiting for the audience availability to become 'Ready' and try again."
                    )
                    yield context.create_timer(
                        context.current_utc_datetime + timedelta(minutes=5)
                    )
                    continue
                else:
                    raise Exception(session["error"])
            break

    return session  # Return the last session's results

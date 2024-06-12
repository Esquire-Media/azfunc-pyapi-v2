#  file path:libs/azure/functions/blueprints/esquire/audiences/egress/xandr/orchestrators/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions import Blueprint
import os, uuid

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def xandr_audience_orchestrator(
    context: DurableOrchestrationContext,
):
    # reach out to audience definition DB - get information pertaining to the xandr audience (segment)
    audience = yield context.call_activity(
        "activity_esquireAudienceXandr_fetchAudience",
        {"id": context.get_input()},
    )

    newSegmentNeeded = not audience["segment"]

    if not newSegmentNeeded:
        # orchestrator that will get the information for the segment associated with the ESQ audience ID
        state = yield context.call_activity(
            "activity_esquireAudienceXandr_getSegment", audience["segment"]
        )
        newSegmentNeeded = not bool(state)

    # if there is no Xandr audience (segment) ID, create one
    if newSegmentNeeded:
        context.set_custom_status("Creating new Xandr Audience (Segment).")
        segment = yield context.call_activity(
            "activity_esquireAudienceXandr_createSegment",
            {
                "parameters": {
                    "advertiser_id": audience["advertiser"],
                },
                "data": {
                    "short_name": "{}_{}".format(
                        "_".join(audience["tags"]),
                        audience["id"],
                    ),
                },
            },
        )
        # Update the database with the new segment ID
        yield context.call_activity(
            "activity_esquireAudienceXandr_putAudience",
            {
                "audience": audience["id"],
                "xandrAudienceId": segment["id"],
            },
        )
        audience["segment"] = segment["id"]

    blob_names = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPaths",
        {
            "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
            "container_name": os.environ["ESQUIRE_AUDIENCE_CONTAINER_NAME"],
            "audience_id": audience["id"],
        },
    )
    yield context.task_all(
        [
            context.call_activity(
                "activity_esquireAudienceXandr_generateAvro",
                {
                    "audience": audience,
                    "source": {
                        "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
                        "container_name": os.environ["ESQUIRE_AUDIENCE_CONTAINER_NAME"],
                        "blob_name": blob_name,
                    },
                    "destination": {
                        "access_key": os.environ["XANDR_SEGMENTS_AWS_ACCESS_KEY"],
                        "secret_key": os.environ["XANDR_SEGMENTS_AWS_SECRET_KEY"],
                        "bucket": os.environ["XANDR_SEGMENTS_S3_BUCKET"],
                        "object_key": "submitted/{}.avro".format(uuid.uuid4().hex),
                    },
                },
            )
            for blob_name in blob_names
        ]
    )

    return {}

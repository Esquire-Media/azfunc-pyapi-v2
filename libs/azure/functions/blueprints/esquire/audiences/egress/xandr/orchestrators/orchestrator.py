#  file path:libs/azure/functions/blueprints/esquire/audiences/egress/xandr/orchestrators/orchestrator.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
import os, uuid

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def xandr_segment_orchestrator(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    # reach out to audience definition DB - get information pertaining to the xandr audience (segment)
    try:
        audience = yield context.call_activity(
            "activity_esquireAudienceXandr_fetchAudience",
            {"id": ingress["audience"]["id"]},
        )
    except:
        return {}

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
                    "segment": {
                        "member_id": int(os.environ["XANDR_MEMBER_ID"]),
                        "short_name": "{}_{}".format(
                            "-".join(audience["tags"]),
                            audience["id"],
                        ),
                    }
                },
            },
        )
        # Update the database with the new segment ID
        yield context.call_activity(
            "activity_esquireAudienceXandr_putAudience",
            {
                "audience": audience["id"],
                "xandrAudienceId": segment,
            },
        )
        audience["segment"] = segment

    blob_names = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPaths",
        {
            "conn_str": ingress["destination"]["conn_str"],
            "container_name": ingress["destination"]["container_name"],
            "audience_id": ingress["audience"]["id"],
        },
    )
    yield context.task_all(
        [
            context.call_activity(
                "activity_esquireAudienceXandr_generateAvro",
                {
                    "audience": audience,
                    "source": {
                        "conn_str": ingress["destination"]["conn_str"],
                        "container_name": ingress["destination"]["container_name"],
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

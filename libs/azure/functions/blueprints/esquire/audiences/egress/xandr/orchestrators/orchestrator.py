#  file path:libs/azure/functions/blueprints/esquire/audiences/egress/xandr/orchestrators/orchestrator.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
import os, uuid

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def xandr_segment_orchestrator(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    
    # Fetch audience definition from the database
    try:
        audience = yield context.call_activity(
            "activity_esquireAudienceXandr_fetchAudience",
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

    newSegmentNeeded = not ingress["audience"]["segment"]

    if not newSegmentNeeded:
        # orchestrator that will get the information for the segment associated with the ESQ audience ID
        state = yield context.call_activity(
            "activity_esquireAudienceXandr_getSegment", ingress["audience"]["segment"]
        )
        newSegmentNeeded = not bool(state)

    # if there is no Xandr audience (segment) ID, create one
    if newSegmentNeeded:
        context.set_custom_status("Creating new Xandr Audience (Segment).")
        segment = yield context.call_activity(
            "activity_esquireAudienceXandr_createSegment",
            {
                "parameters": {
                    "advertiser_id": ingress["audience"]["advertiser"],
                },
                "data": {
                    "segment": {
                        "member_id": int(os.environ["XANDR_MEMBER_ID"]),
                        "short_name": ingress["audience"]["name"],
                    }
                },
            },
        )
        # Update the database with the new segment ID
        yield context.call_activity(
            "activity_esquireAudienceXandr_putAudience",
            {
                "audience": ingress["audience"]["id"],
                "xandrAudienceId": segment,
            },
        )
        ingress["audience"]["segment"] = segment

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
                    "audience": ingress["audience"],
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

    return ingress
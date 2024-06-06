#  file path:libs/azure/functions/blueprints/esquire/audiences/egress/xandr/orchestrators/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
import os, json, random, logging, datetime
import pandas as pd

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def xandr_audience_orchestrator(
    context: DurableOrchestrationContext,
):
    audience_id = context.get_input()
    # ingress="clulpbfdg001v12jixniohdne"

    # reach out to audience definition DB - get information pertaining to the xandr audience (segment)
    ids = yield context.call_activity(
        "activity_esquireAudienceXandr_fetchAudience",
        audience_id,
    )
    
    newSegmentNeeded = not ids["segment"]
    
    if not newSegmentNeeded:
        # orchestrator that will get the information for the segment associated with the ESQ audience ID
        state = yield context.call_activity(
            "activity_esquireAudienceXandr_getSegment",
            ids["segment"]
        )
        newSegmentNeeded = not bool(state)
        
    # if there is no Xandr audience (segment) ID, create one
    if newSegmentNeeded:
        context.set_custom_status("Creating new Xandr Audience (Segment).")
        xandrSegment = yield context.call_activity(
            "activity_esquireAudienceXandr_createSegment",
            {
                "parameters":{
                    "advertiser_id": ids['advertiser'],
                },
                "data":{
                    "short_name": f"{'_'.join(ids['tags'])}_{audience_id}",
                }
            }
        )
        # Update the database with the new segment ID
        yield context.call_activity(
            "activity_esquireAudienceXandr_putAudience",
            {
                "audience": audience_id,
                "xandrAudienceId": xandrSegment["id"],
            },
        )

    
    yield context.call_sub_orchestrator(
        "xandr_audience_avroGenerator",
        {
            "audienceId": audience_id,
            "segmentId": xandrSegment["id"],
            "expiration": ids["expiration"]
        },
    )
    return {}
    
    # get object with all of the blob URLs and how many MAIDs are in that file
    url_maids = yield context.call_activity(
        "activity_esquireAudiencesUtils_getTotalMaids",
        {
            "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
            "container_name": "general",
            # "path_to_blobs": blobs_path,
            # "audience_id": ingress,
        },
    )

    maids_info, blob_count = url_maids
    
    # Create the list of tasks
    context.set_custom_status("Creating avro file for Xandr Audience.")
    # session_id = random.randint(0, 2**32 - 1)

    xandrSegment =  yield context.call_activity(
        "activity_esquireAudienceXandr_generateAvro",
        {
            "xandr_segment_id": 34606932, #static for testing
            "maids_url":maids_info,            
        }
    )
    logging.warning(xandrSegment)
    
    return {}
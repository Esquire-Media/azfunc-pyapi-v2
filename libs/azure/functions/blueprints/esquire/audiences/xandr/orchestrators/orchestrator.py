#  file path:libs/azure/functions/blueprints/esquire/audiences/xandr/orchestrators/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
import os, json, random, logging, datetime
import pandas as pd

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def xandr_audience_orchestrator(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    # ingress="clulpbfdg001v12jixniohdne"

    # reach out to audience definition DB - get information pertaining to the xandr audience (segment)
    ids = yield context.call_activity(
        "activity_esquireAudienceXandr_fetchAudience",
        ingress,
    )

    newAudienceNeeded = not ids["audience"]
    
    if not newAudienceNeeded:
        xandrSegment = yield context.call_sub_orchestrator(
            "_orchestrator_request",
            {
                
            }
        )
        newAudienceNeeded = bool(xandrSegment.get("error", False))
        
    # if there is no Xandr audience (segment) ID
    if newAudienceNeeded:
        context.set_custom_status("Creating new Xandr Audience (Segment).")
        xandrSegment = yield context.call_sub_orchestrator(
            "_orchestrator_request",
            {
                
            }
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
    url_maids = yield context.call_activity(
        "activity_esquireAudiencesUtils_getTotalMaids",
        {
            "conn_str": os.environ["ESQUIRE_AUDIENCE_CONN_STR"],
            "container_name": "general",
            "path_to_blobs": blobs_path,
            "audience_id": ingress,
        },
    )
    
    blob_url, total_maids = url_maids
    logging.warning(f"{blob_url} {total_maids}")

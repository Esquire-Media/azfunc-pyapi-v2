#  file path:libs/azure/functions/blueprints/esquire/audiences/egress/xandr/orchestrators/avroGenerator.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
import os, json, random, logging, datetime
import pandas as pd

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def xandr_audience_avroGenerator(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    
    # Get the folder with the most recent MAIDs (Mobile Advertiser IDs)
    audience_blob_prefix = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPrefix",
        {
            "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
            "container_name": os.environ["ESQUIRE_AUDIENCE_CONTAINER_NAME"],
            "audience_id": ingress['audienceId'],
        },
    )
    
    return {}
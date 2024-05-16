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
    
    # reach out to audience definition DB - get information pertaining to the xandr audience/segment
    ids = yield context.call_activity(
        ""
    )
    
    # activity to get the number of MAIDs
    url_maids = yield context.call_activity(
        "activity_esquireAudiencesMeta_getTotalMaids",
        {
            "conn_str": os.environ["ESQUIRE_AUDIENCE_CONN_STR"],
            "container_name": "general",
            # "blob_name": "audiences/a0H5A00000aZbI1UAK/2023-12-04T18:16:59.757249+00:00/maids.csv",
            "audience_id": ingress,
        },
    )
    blob_url, total_maids = url_maids
    logging.warning(f"{blob_url} {total_maids}")
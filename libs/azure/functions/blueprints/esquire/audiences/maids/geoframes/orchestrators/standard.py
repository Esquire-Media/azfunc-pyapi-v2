# File: libs/azure/functions/blueprints/esquire/audiences/maids/geoframes/orchestrators/standard.py

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import pandas as pd
import uuid
from datetime import datetime
from dateutil.relativedelta import relativedelta

bp = Blueprint()


# main orchestrator for friends and family audiences (suborchestrator for the root)
## one audience at a time
@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudienceMaidsGeoframes_standard(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)
    now = datetime.utcnow()
    today = datetime(now.year, now.month, now.day)
    end = today - relativedelta(days=2)
    start = end - relativedelta(days=90)

    # pass Friends and Family to OnSpot Orchestrator
    onspot = yield context.task_all(
        [
            context.call_sub_orchestrator_with_retry(
                "onspot_orchestrator",
                retry,
                {
                    **ingress["working"],
                    "endpoint": "/save/geoframe/all/devices",
                    "request": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": poly,
                                "properties": {
                                    "name": uuid.uuid4().hex,
                                    "fileName": uuid.uuid4().hex,
                                    "start": start.isoformat(),
                                    "end": end.isoformat(),
                                    "hash": False,
                                },
                            }
                            for poly in batch
                        ],
                    },
                },
            )
            for batch in poly_batches
        ]
    )

    # check if all callbacks succeeded
    if not all([c["success"] for r in onspot for c in r["callbacks"]]):
        # if there are failures, throw exception of what failed in the call
        # TODO: exception for submission failures
        raise Exception([c for r in onspot for c in r["callbacks"] if not c["success"]])

    # merge all of the device files into one file
    yield context.call_activity_with_retry(
        "activity_onSpot_mergeDevices",
        retry,
        {
            "source": ingress["working"],
            "destination": ingress["destination"],
        },
    )

    return {}

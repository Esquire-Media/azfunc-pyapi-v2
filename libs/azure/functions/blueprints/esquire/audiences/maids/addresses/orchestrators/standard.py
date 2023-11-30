# File: libs/azure/functions/blueprints/esquire/audiences/maids/addresses/orchestrators/standard.py

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import pandas as pd
import uuid
from datetime import datetime
from dateutil.relativedelta import relativedelta

bp: Blueprint = Blueprint()


# main orchestrator for friends and family audiences (suborchestrator for the root)
## one audience at a time
@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudienceMaidsAddresses_standard(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    # pass Friends and Family to OnSpot Orchestrator
    onspot = yield context.call_sub_orchestrator_with_retry(
        "onspot_orchestrator",
        retry,
        {
            **ingress["working"],
            "endpoint": "/save/addresses/all/devices",
            "request": {
                "hash": False,
                "name": uuid.uuid4().hex,
                "fileName": uuid.uuid4().hex,
                "fileFormat": {
                    "delimiter": ",",
                    "quoteEncapsulate": True,
                },
                "mappings": {
                    "street": ["delivery_line_1"],
                    "city": ["city_name"],
                    "state": ["state_abbreviation"],
                    "zip": ["zipcode"],
                    "zip4": ["plus4_code"],
                },
                "matchAcceptanceThreshold": 29.9,
                "sources": [
                    ingress["source"].replace("https://", "az://")
                ],
            },
        },
    )

    # check if all callbacks succeeded
    if not all([c["success"] for r in onspot for c in r["callbacks"]]):
        # if there are failures, throw exception of what failed in the call
        # TODO: exception for submission failures
        raise Exception(
            [c for r in onspot for c in r["callbacks"] if not c["success"]]
        )

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

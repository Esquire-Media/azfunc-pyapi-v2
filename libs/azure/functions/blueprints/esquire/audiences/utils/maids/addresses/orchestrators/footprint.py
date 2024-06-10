# File: libs/azure/functions/blueprints/esquire/audiences/maids/addresses/orchestrators/footprint.py

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import pandas as pd
import uuid
from datetime import datetime
from dateutil.relativedelta import relativedelta

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudienceMaidsAddresses_footprint(
    context: DurableOrchestrationContext,
):
    """
    Suborchestrator function for processing Friends and Family audiences.

    This function coordinates the tasks for generating rooftop polygons and processing
    them through the OnSpot Orchestrator. It also handles merging the resultant device files.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context for the orchestration, containing methods for interacting with the Durable Functions runtime.
        - working : dict
            Information about the working blob storage, including blob prefix and other details.
        - source : str
            The source CSV file URL for data processing.
    """
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    # Read data in chunks and orchestrate tasks for rooftop polygon processing
    chunksize = 1000
    poly_batches = yield context.task_all(
        [
            context.call_sub_orchestrator_with_retry(
                "orchestrator_rooftopPolys",
                retry,
                df.dropna(subset=["delivery_line_1"])
                .apply(
                    lambda row: f"{row['delivery_line_1']}, {row['city_name']} {row['state_abbreviation']}, {row['zipcode']}",
                    axis=1,
                )
                .str.upper()
                .to_list(),
            )
            for df in pd.read_csv(
                ingress["source"],
                chunksize=chunksize,
            )
        ]
    )

    # Define time range for OnSpot processing
    now = datetime.utcnow()
    today = datetime(now.year, now.month, now.day)
    end = today - relativedelta(days=2)
    start = end - relativedelta(days=90)

    # Orchestrate OnSpot processing tasks with polygon data
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

    # Check for success in all callbacks from OnSpot Orchestrator
    if not all([c["success"] for r in onspot for c in r["callbacks"]]):
        # Raise an exception if any callback failed
        raise Exception([c for r in onspot for c in r["callbacks"] if not c["success"]])

    # merge all of the device files into one file
    blob_uri = yield context.call_activity_with_retry(
        "activity_onSpot_mergeDevices",
        retry,
        {
            "source": ingress["working"],
            "destination": ingress["destination"],
        },
    )

    return blob_uri

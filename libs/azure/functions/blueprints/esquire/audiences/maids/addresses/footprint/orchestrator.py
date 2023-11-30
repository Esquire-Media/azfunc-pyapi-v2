# File: libs/azure/functions/blueprints/esquire/audiences/maids/addresses/footprint/orchestrator.py

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from fuzzywuzzy import fuzz
from libs.utils.smarty import bulk_validate
import pandas as pd
import uuid
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging

bp: Blueprint = Blueprint()


# main orchestrator for friends and family audiences (suborchestrator for the root)
## one audience at a time
@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudienceMaidsAddresses_footprint(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    # suborchestrator for the rooftop polys
    poly_batches = yield context.task_all(
        [
            # testing for friends and family with sample file
            context.call_sub_orchestrator_with_retry(
                "orchestrator_rooftopPolys",
                retry,
                chunk.dropna(subset=["delivery_line_1"])
                .apply(
                    lambda row: f"{row['delivery_line_1']}, {row['city_name']} {row['state_abbreviation']}, {row['zipcode']}",
                    axis=1,
                )
                .str.upper()
                .to_list(),
            )
            for chunk in pd.read_parquet(
                ingress["source"],
                columns=[
                    "delivery_line_1",
                    "city_name",
                    "state_abbreviation",
                    "zipcode",
                ],
                chunksize=1000,
                encoding_errors="ignore",
            )
        ]
    )

    now = datetime.utcnow()
    today = datetime(now.year, now.month, now.day)
    end = today - relativedelta(days=2)
    start = end - relativedelta(days=90)

    # pass Friends and Family to OnSpot Orchestrator
    onSpot_results = yield context.task_all(
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
    if not all([c["success"] for r in onSpot_results for c in r["callbacks"]]):
        # if there are failures, throw exception of what failed in the call
        # TODO: exception for submission failures
        raise Exception(
            [c for r in onSpot_results for c in r["callbacks"] if not c["success"]]
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


def detect_column_names(df):
    """
    Attempts to automatically detect the address component columns in a sales file
    Returns a slice of the sales data with detected columns for [address, city, state, zip]
    """
    # dictionary of common column headers for address components
    mapping = {
        "street": ["address", "street", "delivery_line_1", "line1", "add"],
        "city": ["city", "city_name"],
        "state": ["state", "st", "state_abbreviation"],
        "zip": ["zip", "zipcode", "postal", "postalcodeid"],
    }

    # find best fit for each address field
    for dropdown, defaults in mapping.items():
        column_scores = [
            max([fuzz.ratio(column.upper(), default.upper()) for default in defaults])
            for column in df.columns
        ]
        best_fit_idx = column_scores.index(max(column_scores))
        best_fit = df.columns[best_fit_idx]
        df = df.rename(columns={best_fit: dropdown})

    return df[["street", "city", "state", "zip"]]

# File: libs/azure/functions/blueprints/esquire/audiences/friends_family/orchestrator.py

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from fuzzywuzzy import fuzz
from libs.utils.smarty import bulk_validate
import pandas as pd
import logging

bp: Blueprint = Blueprint()


# main orchestrator for friends and family audiences (suborchestrator for the root)
## one audience at a time
@bp.orchestration_trigger(context_name="context")
def orchestrator_audience_friendsFamily(
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
                valid_chunk.to_list()[:20],
            )
            for chunk in pd.read_csv(ingress["source"], chunksize=100, encoding_errors="ignore")
            if isinstance((mapped_chunk := detect_column_names(chunk)), pd.DataFrame)
            if isinstance(
                (
                    valid_chunk := bulk_validate(
                        df=mapped_chunk,
                        address_col="street"
                        if "street" in mapped_chunk.columns
                        else None,
                        city_col="city" if "city" in mapped_chunk.columns else None,
                        state_col="state" if "state" in mapped_chunk.columns else None,
                        zip_col="zipcode"
                        if "zipcode" in mapped_chunk.columns
                        else None,
                    )
                    .dropna(subset=["delivery_line_1"])
                    .apply(
                        lambda row: f"{row['delivery_line_1']}, {row['city_name']} {row['state_abbreviation']}, {row['zipcode']}",
                        axis=1,
                    )
                    .str.upper()
                ),
                pd.Series,
            )
        ]
    )
    
    logging.warning(f"Polys: {poly_batches}")

    # # pass Friends and Family to OnSpot Orchestrator
    # yield context.task_all(
    #     [
    #         context.call_sub_orchestrator_with_retry(
    #             "onspot_orchestrator",
    #             retry,
    #             {
    #                 "conn_str": ingress["conn_str"],
    #                 "container": ingress["container_name"],
    #                 "outputPath": "{}/{}/{}".format(
    #                     ingress["blob_prefix"], ingress["audience"]["Id"], "devices"
    #                 ),
    #                 "endpoint": "/save/geoframe/all/devices",
    #                 "request": {}
    #             },
    #         )
    #         for polys in poly_batches
    #     ]
    # )

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

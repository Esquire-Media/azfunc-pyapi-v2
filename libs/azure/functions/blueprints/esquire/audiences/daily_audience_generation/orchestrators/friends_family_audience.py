# File: libs/azure/functions/blueprints/daily_audience_generation/orchestrators/friends_family_audience.py

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from urllib.parse import unquote
from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from fuzzywuzzy import fuzz
from libs.utils.smarty import bulk_validate
import pandas as pd
import os
import logging

bp: Blueprint = Blueprint()


# main orchestrator for friends and family audiences (suborchestrator for the root)
## one audience at a time
@bp.orchestration_trigger(context_name="context")
def suborchestrator_friends_family(context: DurableOrchestrationContext):
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)
    # load the audience addresses file into a dataframe
    blob_client = BlobClient.from_connection_string(
        conn_str=os.environ["ONSPOT_CONN_STR"],
        container_name="general",
        blob_name="{}/{}/{}.csv".format(
            ingress["blob_prefix"],
            ingress["audience"]["Id"],
            ingress["audience"]["Id"],
        ),
    )
    blob_url = (
        unquote(blob_client.url)
        + "?"
        + generate_blob_sas(
            account_name=blob_client.account_name,
            container_name=blob_client.container_name,
            blob_name=blob_client.blob_name,
            account_key=blob_client.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
    )

    # suborchestrator for the rooftop polys
    polys = yield context.task_all(
        [
            # testing for friends and family with sample file
            context.call_sub_orchestrator_with_retry(
                "orchestrator_rooftop_poly",
                retry,
                valid_chunk.to_list()
            )
            for chunk in pd.read_csv(blob_url, chunksize=1000, encoding_errors="ignore")
            if isinstance((mapped_chunk := detect_column_names(chunk)), pd.DataFrame)
            if isinstance((
                valid_chunk := bulk_validate(
                    df=mapped_chunk,
                    address_col="street" if "street" in mapped_chunk.columns else None,
                    city_col="city" if "city" in mapped_chunk.columns else None,
                    state_col="state" if "state" in mapped_chunk.columns else None,
                    zip_col="zipcode" if "zipcode" in mapped_chunk.columns else None,
                )
                .dropna(subset=["delivery_line_1"])
                .apply(
                    lambda row: f"{row['delivery_line_1']}, {row['city_name']} {row['state_abbreviation']}, {row['zipcode']}",
                    axis=1,
                )
                .str.upper()),pd.Series
            ) 
        ]
    )

    logging.warning(f"Polys: {polys}")
    
    # pass New Movers and *Digital Neighbors* to OnSpot Orchestrator
    # yield context.call_sub_orchestrator_with_retry(
    #             "onspot_orchestrator",
    #             retry,
    #             {
    #                 "conn_str": ingress["conn_str"],
    #                 "container": ingress["container"],
    #                 "outputPath": "{}/{}/{}".format(
    #                     ingress["blob_prefix"], ingress["audience"]["Id"], "devices"
    #                 ),
    #                 "endpoint": "/save/addresses/all/devices",
    #                 "request": {
    #                     "hash": False,
    #                     "name": ingress["audience"]["Id"],
    #                     "fileName": ingress["audience"]["Id"],
    #                     "fileFormat": {
    #                         "delimiter": ",",
    #                         "quoteEncapsulate": True,
    #                     },
    #                     "mappings": {
    #                         "street": ["address"],
    #                         "city": ["city"],
    #                         "state": ["state"],
    #                         "zip": ["zipcode"],
    #                         "zip4": ["zip4Code"],
    #                     },
    #                     "matchAcceptanceThreshold": 29.9,
    #                     "sources": [
    #                         blob_client.url.replace("https://", "az://")
    #                         + "?"
    #                         + generate_blob_sas(
    #                             account_name=blob_client.account_name,
    #                             container_name=blob_client.container_name,
    #                             blob_name=blob_client.blob_name,
    #                             account_key=blob_client.credential.account_key,
    #                             permission=BlobSasPermissions(read=True),
    #                             expiry=datetime.utcnow() + relativedelta(days=2),
    #                         )
    #                     ],
    #                 },
    #             },
    #         )
    
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

from libs.azure.functions import Blueprint
import pandas as pd
import os
import json
from io import BytesIO
from azure.durable_functions import (
    DurableOrchestrationClient,
    DurableOrchestrationContext,
    EntityId,
    RetryOptions
)
from azure.storage.blob import BlobServiceClient
from libs.azure.functions.http import HttpRequest, HttpResponse
from sqlalchemy.orm import Session
from libs.data import from_bind
import geojson
import uuid

bp:Blueprint = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_daily_audience_generation(context:DurableOrchestrationContext):
    
    retry = RetryOptions(15000, 3)
    egress = {"instance_id": context.instance_id}

    # call activity to load salesforce objects
    blob_info = yield context.call_activity_with_retry(
        name='activity_load_salesforce',
        retry_options=retry,
        input_={
            **egress
        }
    )
   
    # call activities to add competitor location geojson(s) to each salesforce object
    yield context.task_all(
        [
            context.call_activity_with_retry(
                'activity_load_salesforce_geojsons',
                retry_options=retry,
                input_={
                    **egress,
                    "blob_name":blob['blob_name']
                }
            )
            for blob in blob_info if blob['audience_type']=='Competitor Location'
        ]
    )

    # call activities to retrieve daily Onspot data for each audience's competitor location(s)
    yield context.task_all(
        [
            context.call_activity_with_retry(
                'activity_pull_onspot_data',
                retry_options=retry,
                input_={
                    **egress,
                    "blob_name":blob['blob_name']
                }
            )
            for blob in blob_info if blob['audience_type']=='Competitor Location'
        ]
    )

    return {}
from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import insert_upload_record
from sqlalchemy import create_engine
import os
import logging
from http import HTTPStatus
from azure.functions import HttpResponse

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_ingestData(context: DurableOrchestrationContext):
    settings = context.get_input()
    retry = RetryOptions(15000, 3)

    # 1. Stage + ingest
    yield context.call_activity_with_retry(
        "create_staging_table", 
        {
            **settings
            }
        )
    yield context.call_activity_with_retry(
        "bulk_load_arrow", 
        {
            **settings
            }
        )

    # 2. Address validation (fan-out / fan-in)
    yield context.call_activity_with_retry(
        "enrich_batched_addresses",
        retry,
        {
            'scope':'billing',
            **settings
            }
        )
    yield context.call_activity_with_retry(
        "enrich_batched_addresses",
        retry,
        {
            'scope':'shipping',
            **settings
            }
    )
    # 3.


    # 4. Finish
    yield context.call_activity_with_retry(
        "job_complete", 
        retry,
        {**settings})
    return "OK"
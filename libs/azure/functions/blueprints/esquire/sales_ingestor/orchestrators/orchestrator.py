from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.database_helpers import insert_upload_record
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
    yield context.call_activity_with_retry("create_staging_table", settings)
    yield context.call_activity_with_retry("bulk_load_arrow", settings)

    # 2. Address validation (fan-out / fan-in)
    addresses = yield context.call_activity_with_retry(
        "get_distinct_addresses",
        retry,
        {**settings}
        )
    
    batch = 500
    yield context.task_all([
        context.call_activity_with_retry(
            "validate_addresses", addresses[i:i+batch],
            retry,
            {**settings}
            )
        for i in range(0, len(addresses), batch)
    ])

    # 3. Order-level EAV transform
    orders = yield context.call_activity_with_retry(
        "get_orders",
        retry,
        {**settings}
        )
    yield context.task_all([
        context.call_activity_with_retry(
            "eav_transform", 
            retry,
            {
                **settings,
                "order": o}
            )
        for o in orders
    ])

    # 4. Finish
    yield context.call_activity_with_retry(
        "job_complete", 
        retry,
        {**settings})
    return "OK"
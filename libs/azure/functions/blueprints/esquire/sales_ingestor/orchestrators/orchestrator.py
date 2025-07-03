from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import insert_upload_record
from sqlalchemy import create_engine
import os
import logging
from http import HTTPStatus
from azure.functions import HttpResponse
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.blob import  _arrow_reader
from azure.storage.blob import BlobClient, ContainerClient


bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_ingestData(context: DurableOrchestrationContext):
    settings = context.get_input()
    retry = RetryOptions(15000, 3)

    # 1. Stage + ingest
    conn_str = os.environ['SALES_INGEST_CONN_STR']
    chunk_size = 10 * 1024 * 1024
    container = 'ingest'
    blob_path = settings['blob_url'].split(container)[-1].lstrip('/')

    table_name = 'Staging'

    blob = BlobClient.from_connection_string(
        conn_str,
        container_name=container,
        blob_name=blob_path,
        max_chunk_get_size=chunk_size,            # download tuning
        max_single_get_size=chunk_size,           # 〃
    )

    reader = _arrow_reader(blob, chunk_size)

    yield context.call_activity_with_retry(
        "create_staging_table", 
        {
            "table_name":table_name,
            "schema":reader.schema,
            **settings
            }
        )
    yield context.call_activity_with_retry(
        "bulk_load_arrow", 
        {
            "table_name":table_name,
            "reader":reader,
            **settings
            }
        )

    # 2. Address validation (fan-out / fan-in)
    yield context.call_activity_with_retry(
        "enrich_batched_addresses",
        retry,
        {
            'scope':'billing',
            "staging_table":table_name,
            **settings
            }
        )
    yield context.call_activity_with_retry(
        "enrich_batched_addresses",
        retry,
        {
            'scope':'shipping',
            "staging_table":table_name,
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
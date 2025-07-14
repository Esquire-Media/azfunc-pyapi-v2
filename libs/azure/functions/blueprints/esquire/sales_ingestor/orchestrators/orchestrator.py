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

    try:
        settings = context.get_input()
        retry = RetryOptions(15000, 3)

        # 1. Stage + ingest
        conn_str = os.environ['SALES_INGEST_CONN_STR']
        chunk_size = 10 * 1024 * 1024
        container = 'ingest'
        blob_path = settings['blob_url'].split(container)[-1].lstrip('/')

        table_name = f'staging_{settings['metadata']['upload_id']}'

        blob = BlobClient.from_connection_string(
            conn_str,
            container_name=container,
            blob_name=blob_path,
            max_chunk_get_size=chunk_size,
            max_single_get_size=chunk_size,
        )

        reader = _arrow_reader(blob, chunk_size)

        yield context.call_activity_with_retry(
            "activity_salesIngestor_createStagingTable", 
            {
                "table_name":table_name,
                "schema":reader.schema,
                **settings
                }
            )
        yield context.call_activity_with_retry(
            "activity_salesIngestor_bulkLoadArrow", 
            {
                "table_name":table_name,
                "reader":reader,
                **settings
                }
            )

        # 2. Address validation (fan-out / fan-in)
        yield context.call_activity_with_retry(
            "activity_sales_ingestor_enrichAddresses",
            retry,
            {
                'scope':'billing',
                "staging_table":table_name,
                **settings
                }
            )
        yield context.call_activity_with_retry(
            "activity_sales_ingestor_enrichAddresses",
            retry,
            {
                'scope':'shipping',
                "staging_table":table_name,
                **settings
                }
        )

        # 3. Do the big sql query moving staging data into the EAV tables
        yield context.call_activity_with_retry(
            'activity_salesIngestor_transformToEAV'
        )

    except Exception as e:
        # if any errors are caught, post an error card to teams tagging Ryan and the calling user
        yield context.call_activity(
            "activity_microsoftGraph_postErrorCard",
            {
                "function_name": "esquire-sales-ingestion",
                "instance_id": context.instance_id,
                "owners": [settings["user"]],
                "error": f"{type(e).__name__} : {e}"[:1000],
                "webhook": os.environ["EXCEPTIONS_WEBHOOK_DEVOPS"],
            },
        )
        logging.warning("Error card sent")
        raise e

    logging.warning("All tasks completed.")

    # Purge history related to this instance
    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )



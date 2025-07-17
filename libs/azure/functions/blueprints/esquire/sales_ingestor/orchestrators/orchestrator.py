from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
import os
import logging
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.blob import  _arrow_reader
from azure.storage.blob import BlobClient
import traceback

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_salesIngestor(context: DurableOrchestrationContext):

    try:
        settings = context.get_input()
        retry = RetryOptions(15000, 3)

        # 1. Stage + ingest
        conn_str = os.environ['SALES_INGEST_CONN_STR']
        chunk_size = 10 * 1024 * 1024
        container = 'ingest'
        # blob_path = settings['blob_url'].split(container)[-1].lstrip('/')
        blob_path = settings['metadata']['upload_id']

        table_name = f"staging_{settings['metadata']['upload_id']}"

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
            retry,
            {
                "table_name":table_name,
                "schema":reader.schema,
                **settings
                }
            )
        yield context.call_activity_with_retry(
            "activity_salesIngestor_streamArrow", 
            retry,
            {
                "table_name":table_name,
                "reader":reader,
                **settings
                }
            )

        # 2. Address validation (fan-out / fan-in)
        yield context.call_activity_with_retry(
            "activity_salesIngestor_enrichAddresses",
            retry,
            {
                'scope':'billing',
                "staging_table":table_name,
                **settings
                }
            )
        yield context.call_activity_with_retry(
            "activity_salesIngestor_enrichAddresses",
            retry,
            {
                'scope':'shipping',
                "staging_table":table_name,
                **settings
                }
        )

        # 3. Do the big sql query moving staging data into the EAV tables
        yield context.call_activity_with_retry(
            'activity_salesIngestor_eavTransform',
            retry,
            {
                **settings
                }
        )

        # 4. do cleanup of staging table
        yield context.call_activity_with_retry(
            'activity_salesIngestor_cleanup',
            retry,
            {
                **settings
                }
        )

    except Exception as e:
        logging.error(e)
        full_trace = traceback.format_exc()
        # if any errors are caught, post an error card to teams tagging Ryan and the calling user
        html_body = f"""
            <html>
                <body>
                    <h2 style="color:red;">Sales Ingestion Failure</h2>
                    <p><strong>{type(e).__name__}:</strong> {str(e)}</p>
                    <p><strong>Trace:</strong> {full_trace}</p>
                </body>
            </html>
            """
        yield context.call_activity(
            "activity_microsoftGraph_sendEmail",
            {
                "from_id": "57d355d1-eeb7-45a0-a260-00daceea9f5f",
                "to_addresses": ["matt@esquireadvertising.com"],
                "subject": "esquire-sales-ingestion Failure",
                "message": html_body,
                "content_type": "html",
            },
        )
        logging.warning("Error email sent")
        raise e

    logging.warning("All tasks completed.")

    # Purge history related to this instance
    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )



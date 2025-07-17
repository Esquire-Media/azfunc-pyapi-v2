from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
import os
import logging
from azure.storage.blob import BlobClient
import traceback

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_salesIngestor(context: DurableOrchestrationContext):

    try:
        settings = context.get_input()
        retry = RetryOptions(15000, 3)

        table_name = f"staging_{settings['metadata']['upload_id']}"

        logging.log("[LOG] Creating Staging Table")
        yield context.call_activity_with_retry(
            "activity_salesIngestor_createStagingTable", 
            retry,
            {
                "table_name":table_name,
                **settings
                }
            )
        logging.log("[LOG] Streaming blob to staging table")
        yield context.call_activity_with_retry(
            "activity_salesIngestor_streamArrow", 
            retry,
            {
                "table_name":table_name,
                **settings
                }
            )
        
        # 2. Address validation (fan-out / fan-in)
        logging.log("[LOG] Enriching Billing Addresses")
        yield context.call_activity_with_retry(
            "activity_salesIngestor_enrichAddresses",
            retry,
            {
                'scope':'billing',
                "staging_table":table_name,
                **settings
                }
            )
        logging.log("[LOG] Enriching Shipping Addresses")
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
        logging.log("[LOG] Transforming into EAV tables")
        yield context.call_activity_with_retry(
            'activity_salesIngestor_eavTransform',
            retry,
            {
                **settings
                }
        )

        # 4. do cleanup of staging table
        logging.log("[LOG] Cleaning up staging table")
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




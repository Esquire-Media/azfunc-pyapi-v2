from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
import os
import logging
import traceback

logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.WARNING)

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_salesIngestor(context: DurableOrchestrationContext):

    try:
        settings = context.get_input()
        retry = RetryOptions(15000, 3)

        table_name = f"staging_{settings['metadata']['upload_id']}"

        yield context.call_activity_with_retry(
            "activity_salesIngestor_createStagingTable", 
            retry,
            {
                "table_name":table_name,
                **settings
                }
            )

        yield context.call_activity_with_retry(
            "activity_salesIngestor_streamArrow", 
            retry,
            {
                "table_name":table_name,
                **settings
                }
            )
        
        # 1.5 intermediate cleaning
        yield context.call_activity_with_retry(
            "activity_salesIngestor_intermediate_processing",
            retry,
            {
                "table_name":table_name,
                **settings
            }
        )
        
        # 2. infer data types and alter the fields as necessary
        yield context.call_activity_with_retry(
            "activity_salesIngestor_inferDataTypes",
            retry,
            {
                "table_name":table_name,
                **settings
                }
        )
        
        # 3. Address validation (fan-out / fan-in)
        
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

        # 4. Do the big sql query moving staging data into the EAV tables
        
        yield context.call_activity_with_retry(
            'activity_salesIngestor_eavTransform',
            retry,
            {
                "staging_table":table_name,
                **settings
                }
        )

    except Exception as e:
        logger.error(msg=e)
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
        logger.warning(msg="Error email sent")

        # 5. do cleanup of staging table
        yield context.call_activity_with_retry(
            'activity_salesIngestor_cleanup',
            retry,
            {
                "staging_table":table_name,
                **settings
                }
        )

        raise e


    # 5. do cleanup of staging table
    yield context.call_activity_with_retry(
        'activity_salesIngestor_cleanup',
        retry,
        {
            "staging_table":table_name,
            **settings
            }
    )

    logger.warning(msg="All tasks completed.")

    # Purge history related to this instance
    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )




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
        # Deterministic timestamp and id
        settings["metadata"]["upload_timestamp"] = context.current_utc_datetime.isoformat()
        settings["metadata"]["upload_id"] = context.new_uuid()

        logger.warning(f'[LOG] batch upload id: {settings["metadata"]["upload_id"]}')

        retry = RetryOptions(15000, 3)

        table_name = f"staging_{settings['metadata']['upload_id']}"
        replace_batches = settings.get("replace_batches") or []

        if replace_batches:
            # EAV prelude stores metadata as a sales_batch EAV string. This
            # distinguishes a pending replacement from a usable legacy batch.
            settings["metadata"]["replacement_status"] = "pending"

            # Read-only check. The destructive activity rechecks under locks.
            yield context.call_activity_with_retry(
                "activity_salesIngestor_validateReplacementTargets",
                retry,
                {
                    "new_batch_id": str(settings["metadata"]["upload_id"]),
                    "tenant_id": settings["metadata"]["tenant_id"],
                    "replace_batches": replace_batches,
                },
            )

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
        
        # standardize our field names
        yield context.call_activity_with_retry(
            "activity_salesIngestor_standardizeColumns",
            retry,
            {
                "table_name": table_name,
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
        
        # 3. Address validation (fan-out / fan-in via sub-orchestrator)
        yield context.call_sub_orchestrator(
            "suborchestrator_salesIngestor_enrichAddresses",
            {
                'scope':'billing',
                "staging_table":table_name,
                **settings
            }
        )

        yield context.call_sub_orchestrator(
            "suborchestrator_salesIngestor_enrichAddresses",
            {
                'scope':'shipping',
                "staging_table":table_name,
                **settings
        })

        # 4. Do the big sql query moving staging data into the EAV tables
        yield context.call_sub_orchestrator(
            "suborchestrator_salesIngestor_eav",
            {
                "staging_table": table_name,
                **settings
                }
        )

        if replace_batches:
            replacement_input = {
                "new_batch_id": str(settings["metadata"]["upload_id"]),
                "tenant_id": settings["metadata"]["tenant_id"],
                "replace_batches": replace_batches,
                "staging_table": table_name,
            }
            try:
                yield context.call_sub_orchestrator(
                    "suborchestrator_salesIngestor_replaceBatches",
                    replacement_input,
                )
            except Exception as replacement_error:
                # The DB may have committed even if Durable lost its completion
                # acknowledgment. Compensation holds the SAME new-batch row
                # lock and never deletes an already-applied replacement.
                try:
                    resolution = yield context.call_activity_with_retry(
                        "activity_salesIngestor_compensateFailedReplacement",
                        retry,
                        replacement_input,
                    )
                except Exception:
                    logger.exception(
                        "Replacement recovery requires manual reconciliation: %s",
                        replacement_input["new_batch_id"],
                    )
                    raise replacement_error

                if resolution["status"] != "replacement_already_committed":
                    raise replacement_error

                # The SQL checkpoint is the authoritative proof of success.
                logger.warning(
                    "Recovered committed replacement after lost activity result: %s",
                    replacement_input["new_batch_id"],
                )

        # Attribution dispatch belongs HERE, after the successful replacement
        # transaction (or its replay-safe reconciliation). The supplied code
        # does not include the target attribution call contract.

    except Exception as e:
        logger.error(msg=e, extra={"context": {"PartitionKey": settings["metadata"]["upload_id"]}})
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
        logger.warning(msg="Error email sent", extra={"context": {"PartitionKey": settings["metadata"]["upload_id"]}})

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

    logger.warning(msg="All tasks completed.", extra={"context": {"PartitionKey": settings["metadata"]["upload_id"]}})

    # # Purge history related to this instance
    # yield context.call_sub_orchestrator(
    #     "purge_instance_history",
    #     {"instance_id": context.instance_id},
    # )

import traceback
from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
import logging, os

bp = Blueprint()

# TODO - Once deployed, will need to add smarty keyvault access

@bp.orchestration_trigger(context_name="context")
def orchestrator_pixelPush_root(context: DurableOrchestrationContext):
    """
    Coordinate the batching of all Pixel Push tasks across each client.
    """
    try:
        ingress = context.get_input()
        instance_id = context.instance_id
        retry = RetryOptions(15000,1)

        # parse the pixelRoutes table to get a list of pulls needed to run
        pixel_routes = yield context.call_activity_with_retry(
            "activity_pixelPush_readRoutesTable",
            retry,
            {}
        )

        # for each pixel data pull, coordinate the required tasks
        yield context.task_all([
            context.call_sub_orchestrator_with_retry(
                "orchestrator_pixelPush_job",
                retry,
                {
                    "client":route["PartitionKey"],
                    "data_pull_name":route["RowKey"],
                    "formatting_orchestrator":route["formatting_orchestrator"],
                    "webhook_url":route["url"],
                    "parent_instance": instance_id,
                    **ingress
                }
            ) for route in pixel_routes
        ])

    except Exception as e:
        full_trace = traceback.format_exc()
        html_body = f"""
            <html>
                <body>
                    <h2 style="color:red;">Unmasked Failure</h2>
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
                "subject": "esquire-unmasked-push Failure",
                "message": html_body,
                "content_type": "html",
            },
        )
        raise e

    # Call sub-orchestrator to purge the instance history
    yield context.call_sub_orchestrator_with_retry(
        "purge_instance_history",
        retry,
        {"instance_id": context.instance_id},
    )
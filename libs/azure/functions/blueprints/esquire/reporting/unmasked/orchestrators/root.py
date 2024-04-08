from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from libs.azure.functions import Blueprint
import logging
import os

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
        # if any errors are caught, post an error card to teams tagging Isaac
        yield context.call_activity(
            "activity_microsoftGraph_postErrorCard",
            {
                "function_name": "esquire-unmasked-push",
                "instance_id": context.instance_id,
                "owners":["8489ce7c-e89f-4710-9d34-1442684ce7fe"],
                "error": f"{type(e).__name__} : {e}"[:1000],
                "webhook": os.environ["EXCEPTIONS_WEBHOOK_DEVOPS"],
            },
        )
        logging.warning("Error card sent")
        raise e

    # Call sub-orchestrator to purge the instance history
    yield context.call_sub_orchestrator_with_retry(
        "purge_instance_history",
        retry,
        {"instance_id": context.instance_id},
    )
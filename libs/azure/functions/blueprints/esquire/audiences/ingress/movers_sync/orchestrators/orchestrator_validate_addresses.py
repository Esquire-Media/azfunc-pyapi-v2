# File: libs/azure/functions/blueprints/esquire/audiences/mover_sync/orchestrators/orchestrator_validate_addresses.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from libs.azure.functions import Blueprint

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_moversSync_validateAddresses(context: DurableOrchestrationContext):
    """
    Orchestrator function for validating addresses in the Movers Sync process.

    This function identifies chunks of data that lack validated addresses, then
    processes each chunk to validate the addresses and create corresponding blobs.
    Finally, it returns a list of datasets affected by the new address validations,
    indicating which datasets need new CETAS tables generated.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context for the orchestration, containing methods for interacting with
        the Durable Functions runtime.

    Returns
    -------
    list
        A list of dataset types that have been affected by the new address validations,
        indicating a need for new CETAS table generation.

    """
    retry = RetryOptions(15000, 1)

    # Get egress settings from the context
    ingress = context.get_input()

    # Identify chunks that still need address validation
    missing_data_list = yield context.call_activity_with_retry(
        "activity_moversSync_getMissingChunks",
        retry,
        ingress,
    )

    # Validate addresses for each identified chunk
    for chunk_info in missing_data_list:
        yield context.call_activity_with_retry(
            "activity_moversSync_validateAddressChunk",
            retry,
            {**ingress, "chunk": chunk_info},
        )

    # Return a list of affected datasets for CETAS generation
    return list(set([d["blob_type"] for d in missing_data_list]))

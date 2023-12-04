from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import os
import logging
import traceback

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_moversSync_validateAddresses(context: DurableOrchestrationContext):
    retry = RetryOptions(15000, 1)

    egress = context.get_input()

    # call activity to find the chunks that still need address validation
    missing_data_list = yield context.call_activity_with_retry(
        "activity_moversSync_getMissingChunks",
        retry,
        {
            **egress,
        },
    )

    # for each chunk of missing address-validated info, validate that chunk and create a blob for it
    for chunk_info in missing_data_list:
        yield context.call_activity_with_retry(
            "activity_moversSync_validateAddressChunk",
            retry,
            {
                **egress,
                "chunk":chunk_info
            }
        )

    # return a list of affected datasets that a new CETAS needs to be generated for
    return list(set([d['blob_type'] for d in missing_data_list]))
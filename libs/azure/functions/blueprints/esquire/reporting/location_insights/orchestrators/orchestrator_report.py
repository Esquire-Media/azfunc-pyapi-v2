from azure.durable_functions import Blueprint
from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
import os
import logging
import uuid
import orjson as json
from azure.storage.blob import BlobClient

bp = Blueprint()

# TODO start and end dates not lining up? off by 1 hr?


@bp.orchestration_trigger(context_name="context")
def orchestrator_locationInsights_report(context: DurableOrchestrationContext):
    """
    Once device observations are pulled, the remainder of the work is done by this orchestrator on an individual report basis.
    """

    # if a custom conn string is set, use that. Otherwise use AzureWebJobsStorage
    # commonly used variables
    settings = context.get_input()
    retry = RetryOptions(15000, 1)
    egress = {
        **settings, 
        "instance_id": context.instance_id,
        "today": context.current_utc_datetime.isoformat()
        }

    location_data_list = yield context.task_all(
        [
        context.call_sub_orchestrator(
            name="orchestrator_locationInsights_location",
            input_={
                **egress,
                "locationID":locationID,
                "pull_id":context.instance_id
            }
        )
        for locationID in settings['locationIDs']
        ]
    )

    # call activity to build the Onspot observations payload
    egress["runtime_container"]["output_blob"] = f"{egress['batch_instance_id']}/{context.instance_id}/output"
    output_blob_name = yield context.call_activity_with_retry(
        "activity_locationInsights_buildReport",
        retry,
        {
            **egress, 
            "locations_data": location_data_list,
            "report_id": context.instance_id
        },
    )

    return output_blob_name
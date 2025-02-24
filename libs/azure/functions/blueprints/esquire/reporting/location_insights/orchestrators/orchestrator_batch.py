from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
import logging, os
import uuid

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_locationInsights_batch(context: DurableOrchestrationContext):
    """
    The Root Orchestrator coordinates on a batch level, collecting metadata for each location and sending callback emails and error cards. 
    """

    try:
        # if a custom conn string is set, use that. Otherwise use AzureWebJobsStorage
        conn_str = (
            "LOCATION_INSIGHTS_CONN_STR"
            if "LOCATION_INSIGHTS_CONN_STR" in os.environ.keys()
            else "AzureWebJobsStorage"
        )

        # commonly used variables
        settings = context.get_input()
        retry = RetryOptions(15000, 1)
        egress = {
            **settings,
            "batch_instance_id":context.instance_id,
            "runtime_container": {
                "conn_str": conn_str,
                "container_name": "location-insights",
            },
            "resources_container":{
                "conn_str":conn_str,
                "container_name":"location-insights-resources"
            }
        }

        batch_size = 10
        location_batches = [
            settings["locationIDs"][i : i + batch_size]
            for i in range(0, len(settings["locationIDs"]), batch_size)
        ]

        # parallelize the processing for each report in the batch, so each is assigned its own suborchestrator
        output_blob_names = yield context.task_all(
            [
                context.call_sub_orchestrator(
                    name="orchestrator_locationInsights_report",
                    input_={
                        **{k:v for k,v in egress.items() if k!='locationIDs'},
                        "locationIDs":batch,
                        "batch_id": uuid.uuid4()
                    },
                )
                for batch in location_batches
            ]
        )

        # generate email callback message body
        message_body = yield context.call_activity_with_retry(
            "activity_locationInsights_generateCallback",
            retry,
            {
                **egress,
                "output_blob_names":output_blob_names
            },
        )

        # send the callback email
        yield context.call_activity_with_retry(
            "activity_microsoftGraph_sendEmail",
            retry,
            {
                "from_id": os.environ["O365_EMAIL_ACCOUNT_ID"],
                "to_addresses": [settings["callback"]],
                "subject": f"Location Insights: {settings['name']}",
                "message": message_body,
                "content_type": "HTML",
            },
        )

    except Exception as e:
        # if any errors are caught, post an error card to teams tagging Ryan and the calling user
        yield context.call_activity(
            "activity_microsoftGraph_postErrorCard",
            {
                "function_name": "esquire-location-insights",
                "instance_id": context.instance_id,
                "owners":["8489ce7c-e89f-4710-9d34-1442684ce7fe", egress['user']],
                "error": f"{type(e).__name__} : {e}"[:1000],
                "webhook": os.environ["EXCEPTIONS_WEBHOOK_DEVOPS"],
            },
        )
        logging.warning("Error card sent")
        raise e
    
    # Purge history related to this instance
    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )
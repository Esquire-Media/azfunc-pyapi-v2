# File: libs/azure/functions/blueprints/esquire/audiences/mover_sync/starter.py

from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions import Blueprint
from azure.functions import TimerRequest
import os

bp = Blueprint()

@bp.timer_trigger(arg_name="timer", schedule="0 0 8 * * *")
@bp.durable_client_input(client_name="client")
async def starter_PixelPush(
    timer: TimerRequest, client: DurableOrchestrationClient
):
    """
    Timer-triggered function to start the Unmasked Pixel Push orchestrator.

    This function is triggered via a timer request. It starts a new instance of
    the Unmasked Pixel Push orchestrator function and logs the initiation with relevant details.

    Parameters
    ----------
    req : HttpRequest
        The incoming HTTP request that triggered this function.
    client : DurableOrchestrationClient
        Client object to interact with the Durable Functions runtime.

    Returns
    -------
    HttpResponse
        The response object with details to check the status of the started orchestrator instance.

    """
    account = 'majikrto'

    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_pixelPush_root",
        client_input={
            "access_key": "DISTILLED_ACCESS_KEY",
            "secret_key": "DISTILLED_SECRET_KEY",
            "bucket": "DISTILLED_BUCKET",
            "region": "DISTILLED_REGION",
            "database": "DISTILLED_DATABASE",
            "workgroup": "DISTILLED_WORKGROUP",
            "users_query":get_users_query(account),
            "events_query":get_events_query(account),
            "store_locations_source":{
                "conn_str": "AzureWebJobsStorage",
                "container_name": f"pixel-push",
                "blob_name": f"{account}/store_locations.csv"
            },
            "account":"majikrto",
            "runtime_container":{
                "conn_str": "AzureWebJobsStorage",
                "container_name": f"{os.environ['TASK_HUB_NAME']}-largemessages",
            }
        },
    )

def get_users_query(account:str) -> str:
    return f"""
    SELECT DISTINCT
        hem,
        first_name,
        last_name,
        personal_email,
        mobile_phone,
        personal_phone,
        personal_address,
        personal_address_2,
        personal_city,
        personal_state,
        personal_zip,
        personal_zip4
    FROM pixel.b2c
    WHERE client = '{account}'
    AND CAST(activity_date AS DATE) = date_add('day', -1, current_date);
    """

def get_events_query(account:str) -> str:
    return f"""
    SELECT
        hem,
        event_date,
        ref_url,
        referer_url
    FROM pixel.b2c
    WHERE client = '{account}'
    AND CAST(activity_date AS DATE) = date_add('day', -1, current_date);
    """
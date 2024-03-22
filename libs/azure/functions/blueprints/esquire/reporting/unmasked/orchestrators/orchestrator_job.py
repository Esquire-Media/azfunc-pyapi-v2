from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from libs.azure.functions import Blueprint
import logging
import os
from libs.utils.azure_storage import load_dataframe
from azure.data.tables import TableClient

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_pixelPush_job(context: DurableOrchestrationContext):
    """
    Execute a "load and push" operation for a single Unmasked Pixel pull.

    Ingress
    ----------
    A dictionary containing the following keys:
        
        client : Client name for filtering the pixel data.
        data_pull_name : Name of the data pull (users, events, etc.)
        formatting_orchestrator : [Default = None] Optional formatting steps may be specified by passing an orchestrator name.
        webhook_url : URL to post the final results.

        access_key : Athena parameter
        secret_key : Athena parameter
        bucket : Athena parameter
        region : Athena parameter
        database : Athena parameter
        workgroup : Athena parameter

        runtime_container :
            conn_str : connection string env variable name for the runtime storage container
            container_name : default container name
    """
    
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    # build the Athena query based on the data pull name
    if ingress['data_pull_name'] == 'users':
        query = get_users_query(ingress['client'])
    elif ingress['data_pull_name'] == 'events':
        query = get_events_query(ingress['client'])
    else:
        raise Exception(f"No query defined for data pull named `{ingress['data_pull_name']}`")

    # Execute query to pull pixel data from Athena
    blob_url = yield context.call_sub_orchestrator_with_retry(
        "aws_athena_orchestrator",
        retry,
        {
            **{k: v for k, v in ingress.items() if "query" not in k},
            "query": query,
            "destination": {
                **ingress["runtime_container"],
                "blob_name": f"{ingress['parent_instance']}/{ingress['client']}/{ingress['data_pull_name']}/00_raw",
            },
        },
    )

    # if specified in the pixelRoutes table, call the formatting orchestrator to apply additional changes 
    if ingress['formatting_orchestrator'] and len(ingress['formatting_orchestrator'])>1:
        blob_url = yield context.call_sub_orchestrator_with_retry(
            ingress['formatting_orchestrator'],
            retry,
            {
                "source":blob_url,
                "client":ingress['client'],
                "data_pull_name":ingress['data_pull_name'],
                "runtime_container":ingress['runtime_container'],
                "parent_instance":ingress['parent_instance']
            }

        )

    # Push data to webhook
    yield context.call_activity_with_retry(
        "activity_httpx",
        retry,
        {
            "method":"POST",
            "url":ingress['webhook_url'],
            "data":load_dataframe(blob_url).to_csv(index=False),
            "headers":{
                "data":ingress["data_pull_name"],
                "Content-Type":"text/csv"
            }
        }
    )

    return {}

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
# File: libs/azure/functions/blueprints/async_tasks/helpers.py

from azure.data.tables import TableServiceClient
from azure.durable_functions import DurableOrchestrationClient, EntityId
from dateutil.relativedelta import relativedelta
import math
import os
import uuid
import logging

# Initialize Azure Table Service client using the connection string from the
# AzureWebJobsStorage environment variable
TABLE_SERVICE = TableServiceClient.from_connection_string(
    conn_str=os.environ["AzureWebJobsStorage"]
)

# Initialize table clients for 'agencies', 'advertisers', 'campaigns', 
# 'creatives', and 'flights' tables
TABLE_CLIENTS = {
    "agencies": TABLE_SERVICE.create_table_if_not_exists("agencies"),
    "advertisers": TABLE_SERVICE.create_table_if_not_exists("advertisers"),
    "campaigns": TABLE_SERVICE.create_table_if_not_exists("campaigns"),
    "creatives": TABLE_SERVICE.create_table_if_not_exists("creatives"),
    "flights": TABLE_SERVICE.create_table_if_not_exists("flights"),
    "submissions": TABLE_SERVICE.create_table_if_not_exists("submissions"),
}

async def request_initializer(
    request: dict,
    client: DurableOrchestrationClient,
    response_url: str = None,
):
    """
    Initialize a durable function task with a given request.

    This function generates a unique instance id, signals a durable function entity 
    to initialize its state, and then starts the orchestrator function for the task.

    Parameters
    ----------
    request : dict
        The request to initialize the task with.
    client : DurableOrchestrationClient
        Client to communicate with Azure Durable Functions.
    response_url : str, optional
        URL to use for sending responses, by default None

    Returns
    -------
    str
        The instance id of the started task.
    """
    # Generate an instance ID
    instanceId = str(uuid.uuid4())

    # Use the instance ID to create a state entity
    await client.signal_entity(
        entityId=EntityId(name="roku_async_entity_request", key=f"{instanceId}"),
        operation_name="init",
        operation_input={
            "request": request,
            "links": {
                k: client._replace_url_origin(response_url, v).replace(
                    client._orchestration_bindings.management_urls["id"], instanceId
                )
                if response_url
                else v.replace(
                    client._orchestration_bindings.management_urls["id"], instanceId
                )
                for k, v in client._orchestration_bindings.management_urls.copy().items()
            },
        },
    )

    # Start orchestrator
    await client.start_new(
        orchestration_function_name="roku_async_orchestrator_tasks", instance_id=instanceId
    )

    return instanceId

def calculate_missing_campaigns(state: dict):
    expected_years = set(
        (state["request"]["date_range"]["start"] + relativedelta(years=i))
        for i in range(math.ceil(state["request"]["date_range"]["months"]/12))
    )
    existing_years = set(
        item["Start"]
        for item in state["existing"]["campaigns"]
    )
    return expected_years - existing_years

def calculate_missing_flights(state: dict):
    """
    Calculate missing flights in the state.

    The function compares expected months with the existing ones and 
    returns the difference.

    Parameters
    ----------
    state : dict
        The state to calculate missing flights from.

    Returns
    -------
    set
        The missing months.
    """
    # Calculate expected and existing months for flights
    expected_months = set(
        (state["request"]["date_range"]["start"] + relativedelta(months=i))
        for i in range(state["request"]["date_range"]["months"])
    )
    existing_months = set(
        item["Start"]
        for item in state["existing"]["flights"]
    )
    return expected_months - existing_months

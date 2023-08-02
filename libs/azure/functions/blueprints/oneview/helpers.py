# File: libs/azure/functions/blueprints/async_tasks/helpers.py

from azure.data.tables import TableServiceClient
from azure.storage.blob import BlobServiceClient
from azure.durable_functions import DurableOrchestrationClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from libs.azure.functions.blueprints.oneview.schemas import (
    RequestSchema,
    AdvertiserRecordSchema,
    CreativeRecordSchema,
    CampaignRecordSchema,
    FlightRecordSchema,
)
from typing import Any
import hashlib
import math
import os
import uuid

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


def state(instanceId: str, value: Any = None):
    container = BlobServiceClient.from_connection_string(
        conn_str=os.environ["AzureWebJobsStorage"]
    ).get_container_client(container=f"{os.environ['TASK_HUB_NAME']}-largemessages")
    if not container.exists():
        container.create_container()
    blob = container.get_blob_client(blob=f"{instanceId}/state.json")

    schema = RequestSchema()
    if not value and blob.exists():
        value = schema.loads(blob.download_blob().content_as_text())
    elif not value:
        value = {}
    elif value:
        if isinstance(value, str):
            value = schema.loads(value)
        else:
            value = schema.load(value)

    # Initialize 'existing' field in value if not present
    if "existing" not in value:
        value["existing"] = {}

    # Load the existing advertiser from the Azure Table if available, else set to None
    try:
        value["existing"]["advertiser"] = AdvertiserRecordSchema().load(
            list(
                TABLE_CLIENTS["advertisers"].query_entities(
                    "PartitionKey eq '{}' and OrganizationID eq '{}'".format(
                        str(value["request"]["advertiser"]["tenant"]),
                        value["request"]["advertiser"]["id"],
                    )
                )
            )[0]
        )
    except:
        value["existing"]["advertiser"] = None

    # Load the existing creative from the Azure Table if available, else set to None
    try:
        value["existing"]["creative"] = CreativeRecordSchema().load(
            TABLE_CLIENTS["creatives"].get_entity(
                str(value["existing"]["advertiser"]["RowKey"]),
                hashlib.md5(value["request"]["creative"].encode("utf8")).hexdigest(),
            )
        )
    except:
        value["existing"]["creative"] = None

    # Load the existing campaign from the Azure Table if available, else set to None
    try:
        value["existing"]["campaigns"] = CampaignRecordSchema().load(
            TABLE_CLIENTS["campaigns"].query_entities(
                "PartitionKey eq '{}' and InstanceID eq '{}'".format(
                    str(value["existing"]["advertiser"]["RowKey"]),
                    instanceId,
                )
            ),
            many=True,
        )
    except:
        value["existing"]["campaigns"] = []

    # Load the existing flights from the Azure Table if available, else set to an empty list
    try:
        flights = []
        for campaign in value["existing"]["campaigns"]:
            flights += list(
                TABLE_CLIENTS["flights"].query_entities(
                    "PartitionKey eq '{}'".format(campaign["OneView_CampaignID"])
                )
            )
        value["existing"]["flights"] = FlightRecordSchema().load(
            flights,
            many=True,
        )
        
    except:
        value["existing"]["flights"] = []

    if "flights" not in value["existing"]:
        value["existing"]["flights"] = []

    blob.upload_blob(schema.dumps(value), overwrite=True)
    return schema.dump(value)


def process_state(instanceId: str, operation: str, ingress: Any):
    schema = RequestSchema()
    value = schema.load(state(instanceId))
    # Perform operation based on operation name
    match operation:
        case "error":
            if "errors" in value:
                value["errors"].append(ingress)
            else:
                value["errors"] = [ingress]
        # Lease operation
        case "lease":
            if "lease" in value:
                if (
                    value["lease"]["provider"] == ingress["provider"]
                    and value["lease"]["access_id"] == ingress["access_id"]
                ):
                    value["lease"] = {
                        "expires": datetime.utcnow() + timedelta(minutes=5),
                        "provider": ingress["provider"],
                        "access_id": ingress["access_id"],
                    }
            elif ingress["provider"] and ingress["access_id"]:
                value["lease"] = {
                    "expires": datetime.utcnow() + timedelta(minutes=5),
                    "provider": ingress["provider"],
                    "access_id": ingress["access_id"],
                }
        # Release operation
        case "break":
            if "lease" in value:
                if (
                    value["lease"]["provider"] == ingress["provider"]
                    and value["lease"]["access_id"] == ingress["access_id"]
                ):
                    del value["lease"]
        # Advertiser operation
        case "advertiser":
            # Delete existing entity and create a new one
            for entity in TABLE_CLIENTS["advertisers"].query_entities(
                "Partitionkey eq '{}' and OneView_AdvertiserID eq '{}'".format(
                    str(value["request"]["advertiser"]["tenant"]),
                    ingress["id"],
                )
            ):
                TABLE_CLIENTS["advertisers"].delete_entity(entity)
            # Define new advertiser data
            data = {
                "PartitionKey": str(value["request"]["advertiser"]["tenant"]),
                "RowKey": str(uuid.uuid4()),
                "OrganizationID": value["request"]["advertiser"]["id"],
                "OrganizationName": value["request"]["advertiser"]["name"],
                "Domain": value["request"]["advertiser"]["domain"],
                "ContentCategory": value["request"]["advertiser"]["category"],
                "OneView_OrgID": ingress["organization_id"],
                "OneView_AdvertiserID": ingress["id"],
            }
            # Create new advertiser entity and update the existing advertiser
            TABLE_CLIENTS["advertisers"].create_entity(data)
            value["existing"]["advertiser"] = AdvertiserRecordSchema().load(data)
        # Creative MD5 operation
        case "creative_md5":
            value["md5"] = ingress
            # Load the existing creative if available
            try:
                value["existing"]["creative"] = CreativeRecordSchema().load(
                    TABLE_CLIENTS["creatives"].get_entity(
                        str(value["existing"]["advertiser"]["RowKey"]), ingress
                    )
                )
            except:
                pass
        # Creative operation
        case "creative":
            if len([c for c in TABLE_CLIENTS["creatives"].query_entities(
                "OneView_CreativeID eq '{}'".format(
                    ingress["id"],
                )
            )]) == 0:
                # Define new creative data
                data = {
                    "PartitionKey": str(value["existing"]["advertiser"]["RowKey"]),
                    "RowKey": hashlib.md5(
                        value["request"]["creative"].encode("utf8")
                    ).hexdigest(),
                    "OneView_CreativeID": ingress["id"],
                    "Landing_Page": value["request"]["landing_page"],
                }
                # Create new creative entity and update the existing creative
                TABLE_CLIENTS["creatives"].create_entity(data)
                value["existing"]["creative"] = CreativeRecordSchema().load(data)
        # Campaign operation
        case "campaign":
            if len([c for c in TABLE_CLIENTS["campaigns"].query_entities(
                "OneView_CampaignID eq '{}'".format(
                    ingress["id"],
                )
            )]) == 0:
                # Define new campaign data
                data = {
                    "PartitionKey": str(value["existing"]["advertiser"]["RowKey"]),
                    "RowKey": str(uuid.uuid4()),
                    "InstanceID": instanceId,
                    "OneView_CampaignID": ingress["id"],
                    "CPM_Client": value["request"]["budget"]["cpm_client"],
                    "CPM_Tenant": value["request"]["budget"]["cpm_tenant"],
                    "Monthly_Impressions": value["request"]["budget"][
                        "monthly_impressions"
                    ],
                    "Start": (
                        value["request"]["date_range"]["start"]
                        + relativedelta(years=len(value["existing"]["campaigns"]))
                    ).isoformat(),
                    "End": (
                        value["request"]["date_range"]["end"]
                        if value["request"]["date_range"]["start"]
                        + relativedelta(years=len(value["existing"]["campaigns"]) + 1)
                        > value["request"]["date_range"]["end"]
                        else value["request"]["date_range"]["start"]
                        + relativedelta(years=len(value["existing"]["campaigns"]) + 1)
                    ).isoformat(),
                }
                # Create new campaign entity and update the existing campaign
                TABLE_CLIENTS["campaigns"].create_entity(data)
                value["existing"]["campaigns"].append(CampaignRecordSchema().load(data))
        # Flight operation
        case "flight":
            if len([c for c in TABLE_CLIENTS["flights"].query_entities(
                "OneView_FlightID eq '{}'".format(
                    ingress["id"],
                )
            )]) == 0:
                # Delete existing entity and create a new one
                for entity in TABLE_CLIENTS["flights"].query_entities(
                    "PartitionKey eq '{}' and Start eq '{}' and End eq '{}'".format(
                        ingress["campaign_id"],
                        ingress["start"],
                        ingress["end"],
                    )
                ):
                    TABLE_CLIENTS["flights"].delete_entity(entity)
                data = {
                    "PartitionKey": ingress["campaign_id"],
                    "RowKey": str(uuid.uuid4()),
                    "OneView_FlightID": ingress["id"],
                    "Start": ingress["start"],
                    "End": ingress["end"],
                }
                TABLE_CLIENTS["flights"].create_entity(data)
                value["existing"]["flights"] = FlightRecordSchema().load(
                    list(
                        TABLE_CLIENTS["flights"].query_entities(
                            "PartitionKey eq '{}'".format(ingress["campaign_id"])
                        )
                    ),
                    many=True,
                )
        case _:
            pass

    return state(instanceId, schema.dump(value))


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
    state(
        instanceId=instanceId,
        value={
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
        orchestration_function_name="oneview_orchestrator_tasks",
        instance_id=instanceId,
    )

    return instanceId


def calculate_missing_campaigns(state: dict):
    expected_years = set(
        (state["request"]["date_range"]["start"] + relativedelta(years=i))
        for i in range(math.ceil(state["request"]["date_range"]["months"] / 12))
    )
    existing_years = set(item["Start"] for item in state["existing"]["campaigns"])
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
    existing_months = set(item["Start"] for item in state["existing"]["flights"])
    return expected_months - existing_months

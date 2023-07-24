# File: libs/azure/functions/blueprints/async_tasks/entities/request.py

from azure.durable_functions import DurableEntityContext
from libs.azure.functions.blueprints.roku_async.helpers import TABLE_CLIENTS
from libs.azure.functions.blueprints.roku_async.schemas import (
    RequestSchema,
    AdvertiserRecordSchema,
    CreativeRecordSchema,
    CampaignRecordSchema,
    FlightRecordSchema,
)
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
import uuid

bp = Blueprint()


# Define a Durable Entity function that maintains the state of a request
@bp.logger()
@bp.entity_trigger(context_name="context")
def roku_async_entity_request(context: DurableEntityContext):
    """
    Entity function to maintain the state of a request, perform certain operations,
    and manage entities in Azure Table storage.

    Parameters
    ----------
    context : DurableEntityContext
        Provides context for the Durable Entity. Includes the state and operations.

    Returns
    -------
    None
    """

    # Get the current state of the entity, or initialize it to the input value
    value = context.get_state(context.get_input)

    # normalize value state using RequestSchema
    schema = RequestSchema()
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
                value["md5"],
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
                    context.entity_key,
                )
            ),
            many=True,
        )
    except:
        value["existing"]["campaigns"] = []

    # Load the existing flights from the Azure Table if available, else set to an empty list
    try:
        for campaign in value["existing"]["campaigns"]:
            value["existing"]["flights"] = FlightRecordSchema().load(
                list(
                    TABLE_CLIENTS["flights"].query_entities(
                        "PartitionKey eq '{}'".format(campaign["OneView_CampaignID"])
                    )
                ),
                many=True,
            )
    except:
        value["existing"]["flights"] = []

    if "flights" not in value["existing"]:
        value["existing"]["flights"] = []

    ingress = context.get_input()

    # Perform operation based on operation name
    match context.operation_name:
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
        case "release":
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
            # Define new creative data
            data = {
                "PartitionKey": str(value["existing"]["advertiser"]["RowKey"]),
                "RowKey": value["md5"],
                "OneView_CreativeID": ingress["id"],
                "Landing_Page": value["request"]["landing_page"],
            }
            # Create new creative entity and update the existing creative
            TABLE_CLIENTS["creatives"].create_entity(data)
            value["existing"]["creative"] = CreativeRecordSchema().load(data)
        # Campaign operation
        case "campaign":
            # Define new campaign data
            data = {
                "PartitionKey": str(value["existing"]["advertiser"]["RowKey"]),
                "RowKey": str(uuid.uuid4()),
                "InstanceID": context.entity_key,
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

    # Set the result of the operation to the updated state
    context.set_result(schema.dumps(value))

    # Update the state of the entity
    context.set_state(schema.dumps(value))

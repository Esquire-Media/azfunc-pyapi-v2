from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationClient
from libs.utils.pydantic.address import EsqId
from libs.utils.pydantic.time import Date
from libs.utils.pydantic.email import EmailAddress
import os
import json
import jwt
from azure.data.tables import TableClient
import logging
from pydantic import BaseModel, conlist
from typing import Optional
from libs.utils.logging import AzureTableHandler
from pydantic import validator
from libs.utils.oauth2.tokens.microsoft import ValidateMicrosoft
from libs.utils.oauth2.tokens import TokenValidationError

bp = Blueprint()

# initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("locationInsights.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)


@bp.route(route="esquire/location_insights/starter", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def starter_locationInsights(req: HttpRequest, client: DurableOrchestrationClient):
    logger = logging.getLogger("locationInsights.logger")

    # load the request payload as a Pydantic object
    payload = HttpRequest.pydantize_body(req, LocationInsightsPayload).model_dump()

    # validate the MS bearer token to ensure the user is authorized to make requests
    try:
        validator = ValidateMicrosoft(
            tenant_id=os.environ['MS_TENANT_ID'], 
            client_id=os.environ['MS_CLIENT_ID']
        )
        headers = validator(req.headers.get('authorization'))
    except TokenValidationError as e:
        return HttpResponse(status_code=401, body=f"TokenValidationError: {e}")

    # extract user information from bearer token metadata
    payload['user'] = headers['oid']
    payload['callback'] = headers['preferred_username']

    # start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_locationInsights_batch",
        client_input=payload,
    )

    # add instance info to the logging table for usage metrics
    # ensure that each value is json serializable
    logger.info(
        msg="started",
        extra={"context": {"PartitionKey": "locationInsights", "RowKey": instance_id, **{k:v if isinstance(v, str) else json.dumps(v) for k,v in payload.items()}}},
    )

    # Return a response that includes the status query URLs
    return client.create_check_status_response(req, instance_id)

class LocationInsightsPayload(BaseModel):
    """
    This class ingests and validates the payload elements for Location Insights reports, with type enforcement.
    """
    
    @validator(__field="creativeSet", pre=False)
    def check_if_valid_creative_set(cls, v:str):
        """
        Verify that the passed creative set exists in the assets table.
        """
        # if empty string is passed for key `creativeSet`, apply the default value
        if isinstance(v, str) and len(v) == 0:
            return 'Furniture Lifestyle'

        # connect to assets table (used for validating the creativeSet parameter)
        creativeSets = TableClient.from_connection_string(
            conn_str=os.getenv("LOCATION_INSIGHTS_CONN_STR", os.environ["AzureWebJobsStorage"]),
            table_name="locationInsightsAssets",
        ).query_entities(f"PartitionKey eq 'creativeSet'", select=["RowKey"])
        # throw exception if value does not exist in the creativeSets assets table
        assert v in [e["RowKey"] for e in creativeSets]
        
        return v
    
    @validator(__field="promotionSet", pre=False)
    def check_if_valid_promotion_set(cls, v:str):
        """
        Verify that the passed promotion set exists in the assets table.
        """
        # if empty string is passed for key `creativeSet`, apply the default value
        if isinstance(v, str) and len(v) == 0:
            return 'HFA Partner'

        # connect to assets table (used for validating the promotionSet parameter)
        promotionSets = TableClient.from_connection_string(
            conn_str=os.getenv("LOCATION_INSIGHTS_CONN_STR", os.environ["AzureWebJobsStorage"]),
            table_name="locationInsightsAssets",
        ).query_entities(f"PartitionKey eq 'promotionSet'", select=["RowKey"])
        # throw exception if value does not exist in the promotionSets assets table
        assert v in [e["RowKey"] for e in promotionSets]
        
        return v
    
    @validator(__field="template", pre=False)
    def check_if_valid_template(cls, v:str):
        """
        Verify that the passed template exists in the assets table.
        """
        # if empty string is passed for key `creativeSet`, apply the default value
        if isinstance(v, str) and len(v) == 0:
            return 'Retail'

        # connect to assets table (used for validating the template parameter)
        templates = TableClient.from_connection_string(
            conn_str=os.getenv("LOCATION_INSIGHTS_CONN_STR", os.environ["AzureWebJobsStorage"]),
            table_name="locationInsightsAssets",
        ).query_entities(f"PartitionKey eq 'template'", select=["RowKey"])
        # throw exception if value does not exist in the templates assets table
        assert v in [e["RowKey"] for e in templates]
        
        return v

    name: str
    endDate: Date
    locationIDs: conlist(EsqId, min_length=1)
    creativeSet: Optional[str] = "Furniture Lifestyle"
    promotionSet: Optional[str] = "Unmasked"
    template: Optional[str] = "Retail"
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationClient
from libs.utils.pydantic.address import AddressComponents, AddressGeocoded
from libs.utils.pydantic.email import EmailAddress
import os
import json
from azure.data.tables import TableServiceClient, TableClient
import logging
import pandas as pd
from pydantic import BaseModel, conlist, AfterValidator, ValidationError
from typing import Annotated, List, Union, Optional
from libs.utils.logging import AzureTableHandler
from uuid import uuid4
from pydantic import validator

bp = Blueprint()

# initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("ryans.magical.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)


@bp.route(route="esquire/campaign_proposal/starter", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def example_http(req: HttpRequest, client: DurableOrchestrationClient):
    logger = logging.getLogger("ryans.magical.logger")

    # load the request payload as a Pydantic object
    payload = HttpRequest.pydantize_body(req, CampaignProposalPayload).model_dump()

    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="esquire_campaign_proposal_orchestrator_root",
        client_input=payload,
    )

    # add instance info to the logging table for usage metrics
    # ensure that each value is json serializable
    logger.info(
        msg="started",
        extra={"context": {"PartitionKey": "index", "RowKey": instance_id, **{k:v if isinstance(v, str) else json.dumps(v) for k,v in payload.items()}}},
    )

    # Return a response that includes the status query URLs
    return client.create_check_status_response(req, instance_id)


class CampaignProposalPayload(BaseModel):
    """
    This class ingests and validates the payload elements for Campaign Proposal, with type enforcement.
    """
    @validator(__field="creativeSet", pre=False)
    def check_if_valid_creative_set(cls, v:str):
        """
        Verify that the passed creative set exists in the assets table.
        """
        # connect to assets table (used for validating the creativeSet parameter)
        creativeSets = TableClient.from_connection_string(
            conn_str=os.environ["ESQUIRE_REPORTS_CONN_STR"],
            table_name="campaignproposalassets",
        ).query_entities(f"PartitionKey eq 'creativeSet'", select=["RowKey"])
        # throw exception if value does not exist in the creativeSets assets table
        if v not in [e["RowKey"] for e in creativeSets]:
            raise ValidationError(
                f"Asset '{v}' was not found in partition 'creativeSet'."
            )

    @validator(__field="moverRadii", pre=False)
    def check_if_valid_mover_radii(cls, v:list):
        """
        Verify that the mover radii list is properly ordered and within the correct bounds.
        """
        lower_bound = 1
        upper_bound = 50
        if v[0] < lower_bound or v[0] >= v[1] or v[1] >= v[2] or v[2] > upper_bound:
            raise ValidationError(
                f"Parameter 'moverRadii' must be a list of integers, in increasing order, with values between {lower_bound} and {upper_bound} (miles)."
            )

    name: str
    categoryIDs: conlist(int, min_length=1)
    addresses: conlist(Union[AddressComponents, AddressGeocoded], min_length=1)
    callback: EmailAddress
    user: Optional[str] = None
    creativeSet: Optional[str] = "Default"
    moverRadii: Optional[conlist(int, min_length=3, max_length=3)] = [5,10,15]
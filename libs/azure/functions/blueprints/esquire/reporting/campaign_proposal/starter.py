from azure.data.tables import TableClient
from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.functions import HttpRequest, HttpResponse
from libs.utils.logging import AzureTableHandler
from libs.utils.oauth2.tokens.microsoft import ValidateMicrosoft
from libs.utils.oauth2.tokens import TokenValidationError
from libs.utils.pydantic.address import AddressComponents, AddressGeocoded
from pydantic import BaseModel, conlist, validator
from typing import Union, Optional
import orjson as json, logging, os

bp = Blueprint()

# initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("campaignProposal.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)


@bp.route(route="esquire/campaign_proposal/starter", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def starter_campaignProposal(req: HttpRequest, client: DurableOrchestrationClient):
    logger = logging.getLogger("campaignProposal.logger")

    # load the request payload as a Pydantic object
    payload = CampaignProposalPayload.model_validate_json(req.get_body()).model_dump()

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

    logging.warning(payload)
    
    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_campaignProposal_root",
        client_input=payload,
    )

    # add instance info to the logging table for usage metrics
    # ensure that each value is json serializable
    logger.info(
        msg="started",
        extra={"context": {"PartitionKey": "campaignProposal", "RowKey": instance_id, **{k:v if isinstance(v, str) else json.dumps(v).decode() for k,v in payload.items()}}},
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
        # if empty string is passed for key `creativeSet`, apply the default value
        if isinstance(v, str) and len(v) == 0:
            return 'Default'

        # connect to assets table (used for validating the creativeSet parameter)
        creativeSets = TableClient.from_connection_string(
            conn_str=os.getenv("CAMPAIGN_PROPOSAL_CONN_STR", os.environ["AzureWebJobsStorage"]),
            table_name="campaignProposalAssets",
        ).query_entities(f"PartitionKey eq 'creativeSet'", select=["RowKey"])
        # throw exception if value does not exist in the creativeSets assets table
        assert v in [e["RowKey"] for e in creativeSets]
        
        return v

    @validator(__field="moverRadii", pre=False)
    def check_if_valid_mover_radii(cls, v:list):
        """
        Verify that the mover radii list is properly ordered and within the correct bounds.
        """
        lower_bound = 1
        upper_bound = 50
        assert (
            v[0] >= lower_bound 
            and v[0] < v[1] 
            and v[1] < v[2] 
            and v[2] <= upper_bound
        )
        return v

    name: str
    categoryIDs: conlist(int, min_length=1)
    addresses: conlist(Union[AddressComponents, AddressGeocoded], min_length=1)
    creativeSet: Optional[str] = "Default"
    moverRadii: Optional[conlist(int, min_length=3, max_length=3)] = [5,10,15]
    optional_slides: Optional[conlist(str, min_length=0)] = ['new_mover', 'in_market_shopper', 'pricing', 'next_steps']
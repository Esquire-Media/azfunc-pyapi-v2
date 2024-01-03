from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationClient
import logging
from pydantic import BaseModel, validator
import json
from libs.utils.logging import AzureTableHandler
from libs.utils.dicts import flatten
import azure.functions as func
import uuid

bp = Blueprint()

# initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("googleLeadsForm.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)

@bp.route(route="esquire/google/leads_form/starter", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def starter_googleLeadsForm(req: HttpRequest, client: DurableOrchestrationClient):
    logger = logging.getLogger("googleLeadsForm.logger")
    instance_id = str(uuid.uuid4())

    # load the request payload as a Pydantic object
    payload = HttpRequest.pydantize_body(req, GoogleLeadsFormPayload).model_dump()
    logging.warning(payload)

    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_googleLeadsForm",
        client_input=payload,
    )

    # add instance info to the logging table for usage metrics
    # ensure that each value is json serializable
    logger.info(
        msg="started",
        extra={
            "context": {
                "PartitionKey": "googleLeadsForm",
                "RowKey": instance_id,
                **{
                    k: v if isinstance(v, str) else json.dumps(v) for k, v in flatten(dictionary=payload, separator='.').items()
                },
            }
        },
    )

    # create a custom response since Google requires a strict 200 status for webhooks but the default durable response gives a 202.
    response_uris = client.get_client_response_links(
        request=req, instance_id=instance_id
    )
    return HttpResponse(
        body=json.dumps(
            {
                "statusQueryGetUri": response_uris["statusQueryGetUri"].replace('http://','https://'),
            }
        ),
        headers={"Content-Type": "application/json"},
        status_code=200,
    )

class ColumnData(BaseModel):
    column_name: str
    string_value: str
    column_id: str


class GoogleLeadsFormPayload(BaseModel):
    @validator(__field="user_column_data", pre=False)
    def transform_user_column_data(cls, v):
        return {item.column_name: item.string_value for item in v}

    lead_id:str
    user_column_data: list[ColumnData]
    api_version:str
    form_id:int
    campaign_id:int
    google_key:str
    is_test:bool
    gcl_id:str
    adgroup_id:int
    creative_id:int
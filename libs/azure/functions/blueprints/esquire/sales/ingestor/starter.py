from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.functions import HttpRequest, HttpResponse
from libs.utils.logging import AzureTableHandler
from libs.utils.oauth2.tokens.microsoft import ValidateMicrosoft
from libs.utils.oauth2.tokens import TokenValidationError
import orjson as json, logging, os



bp: Blueprint = Blueprint()

# initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("locationInsights.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)


@bp.route(route="esquire/sales/ingestor", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def sales_ingestion_starter(req: HttpRequest, client: DurableOrchestrationClient):
    logger = logging.getLogger("locationInsights.logger")

    # load the request payload as a Pydantic object
    payload = req.get_body()

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
    
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_ingestData",
        client_input=payload,
    )

    # add instance info to the logging table for usage metrics
    # ensure that each value is json serializable
    logger.info(
        msg="started",
        extra={"context": {"PartitionKey": "locationInsights", "RowKey": instance_id, **{k:v if isinstance(v, str) else json.dumps(v).decode() for k,v in payload.items()}}},
    )

    return client.create_check_status_response(req, instance_id)
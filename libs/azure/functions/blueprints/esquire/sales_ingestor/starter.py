from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.functions import HttpRequest, HttpResponse
from libs.utils.logging import AzureTableHandler
from libs.utils.oauth2.tokens.microsoft import ValidateMicrosoft
from libs.utils.oauth2.tokens import TokenValidationError
import orjson as json, logging, os



bp: Blueprint = Blueprint()

# initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("salesIngestor.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)


@bp.route(route="esquire/sales_ingestor/starter", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def sales_ingestion_starter(req: HttpRequest, client: DurableOrchestrationClient):
    logger = logging.getLogger("salesIngestor.logger")

    # load the request payload
    # load and parse the request payload
    payload_bytes = req.get_body()
    try:
        payload = json.loads(payload_bytes)
    except Exception:
        return HttpResponse(status_code=400, body="Invalid JSON payload.")
    
    #validate the format of the incoming payload
    valid_schema = validate_schema(payload)
    if valid_schema.get("status") == "error":
        return HttpResponse(
            status_code=400,
            body=f"Malformed payload: {valid_schema['error']}"
            )

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
    
    # start a new orchestration
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_ingestData",
        client_input=payload,
    )

    # add instance info to the logging table for usage metrics
    # ensure that each value is json serializable
    logger.info(
        msg="started",
        extra={"context": {"PartitionKey": "salesIngestor", "RowKey": instance_id, **{k:v if isinstance(v, str) else json.dumps(v).decode() for k,v in payload.items()}}},
    )

    return client.create_check_status_response(req, instance_id)

def validate_schema(settings):
    # primary sections
    for section in [
        "fields",
        "metadata",
        ]:
        if not settings.get(section,{}):
            return {"status": "error", "error": f"Missing {section}"}
        
    # required metadata information
    for metadata in [
        "tenant_id",
        "upload_id",
        "uploader",
        "upload_timestamp"
        ]:
        if not settings['metadata'].get(metadata,""):
            return {"status": "error", "error": f"Missing {metadata} metadata"}

    # required field sections
    for subsection in [
        "billing",
        "shipping",
        "order_info"
        ]:
        if not settings['fields'].get(section,""):
            return {"status": "error", "error": f"Missing {subsection} fields"}
        
    for field in [
        "order_num",
        "sale_date"
        ]:
        if not settings['fields']['order_info'].get(field,""):
            return {"status": "error", "error": f"Missing {field} order field"}
        
    # success
    return {"status":"successful"}
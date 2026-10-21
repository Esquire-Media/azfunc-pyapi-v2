from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.functions import HttpRequest, HttpResponse
from libs.utils.oauth2.tokens.microsoft import ValidateMicrosoft
from libs.utils.oauth2.tokens import TokenValidationError
from libs.utils.logging import AzureTableHandler
from uuid import UUID
import orjson as json, logging, os

bp = Blueprint()
_MAX_REPLACEMENT_BATCHES = 10

# initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("salesIngestor.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)


@bp.route(route="esquire/sales_ingestor/starter", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def starter_salesIngestor(req: HttpRequest, client: DurableOrchestrationClient):
    logger = logging.getLogger("salesIngestor.logger")
    logger.setLevel(logging.INFO)

    try:
        payload = json.loads(req.get_body().decode("utf-8"))
    except Exception:
        logger.exception("Invalid JSON payload")
        return HttpResponse(status_code=400, body="Invalid JSON payload.")
    
    #validate the format of the incoming payload
    valid_schema = validate_schema(payload)
    if valid_schema.get("status") == "error":
        return HttpResponse(
            status_code=400,
            body=f"Malformed payload: {valid_schema['error']}"
            )

    # Normalize optional batch IDs; the orchestrator handles the replacement branch.
    if "replace_batches" in payload:
        payload["replace_batches"] = [
            str(UUID(batch_id)) for batch_id in payload["replace_batches"]
        ]

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
    payload['metadata']['uploader'] = headers.get("preferred_username")
    # Start the same orchestration for both ordinary and replacement uploads.
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_salesIngestor",
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
    if not isinstance(settings, dict):
        return {"status": "error", "error": "Payload must be an object"}
    # primary sections
    for section in [
        "fields",
        "metadata",
        ]:
        if not isinstance(settings.get(section), dict) or not settings[section]:
            return {"status": "error", "error": f"Missing or invalid {section}"}
        
    # required metadata information
    for metadata in [
        "tenant_id",
        "blob_id"
        ]:
        value = settings['metadata'].get(metadata)
        if not isinstance(value, str) or not value.strip():
            return {"status": "error", "error": f"Missing or invalid {metadata} metadata"}

    # required field sections
    for subsection in [
        "billing",
        "shipping",
        "order_info"
        ]:
        if not isinstance(settings['fields'].get(subsection), dict) or not settings['fields'][subsection]:
            return {"status": "error", "error": f"Missing or invalid {subsection} fields"}
        
    for field in [
        "order_num",
        "sale_date"
        ]:
        value = settings['fields']['order_info'].get(field)
        if not isinstance(value, str) or not value.strip():
            return {"status": "error", "error": f"Missing or invalid {field} order field"}

    if "replace_batches" in settings:
        batch_ids = settings["replace_batches"]
        if not isinstance(batch_ids, list) or len(batch_ids) > _MAX_REPLACEMENT_BATCHES:
            return {"status": "error", "error": f"replace_batches must be a list of at most {_MAX_REPLACEMENT_BATCHES} UUIDs"}
        canonical = []
        for batch_id in batch_ids:
            if not isinstance(batch_id, str):
                return {"status": "error", "error": "Replacement IDs must be UUID strings"}
            try:
                canonical.append(str(UUID(batch_id)))
            except ValueError:
                return {"status": "error", "error": f"Invalid replacement batch UUID: {batch_id}"}
        if len(set(canonical)) != len(canonical):
            return {"status": "error", "error": "Duplicate IDs in replace_batches"}

    return {"status":"successful"}
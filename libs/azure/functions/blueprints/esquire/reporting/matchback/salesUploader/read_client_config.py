from azure.data.tables import TableClient
from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.functions import HttpRequest, HttpResponse
from libs.utils.oauth2.tokens import TokenValidationError
from libs.utils.oauth2.tokens.microsoft import ValidateMicrosoft
from pydantic import BaseModel
import orjson as json, os, pandas as pd

bp = Blueprint()

# NOTE : This architecture assumes only one sales file schema per MS Group.

@bp.route(route="esquire/salesUploader/readClientConfig", methods=["POST", "OPTIONS"])
@bp.durable_client_input(client_name="client")
async def http_salesUploader_readClientConfig(
    req: HttpRequest, client: DurableOrchestrationClient
):
    """
    Handles a POST request to retrieve the sales column mappings from the previous file upload for a specified client, using the Sales Uploader Teams app. 
    This endpoint validates the Microsoft bearer token from the request header for authorization and queries an Azure Table Storage to fetch the client's configuration.
    If no prior configuration exists or if the token validation fails, appropriate responses are returned.

    HTTP Payload:
    - group_id: The MS Graph ID of the Group which the sales file belongs to.

    Parameters:
    - req (HttpRequest): The request object, including headers and payload. The payload must contain a `group_id` to identify the client's configuration.
    - client (DurableOrchestrationClient): A client object to interact with Azure Durable Functions (unused in this function but required by the decorator).

    Returns:
    - HttpResponse: A JSON-formatted response containing the client's sales column mappings if found, an empty object if no previous configuration exists, or an error message if the token validation fails.

    Raises:
    - TokenValidationError: If the Microsoft bearer token validation fails.
    """
    
    # validate the MS bearer token to ensure the user is authorized to make requests
    try:
        validator = ValidateMicrosoft(
            tenant_id=os.environ["MS_TENANT_ID"], client_id=os.environ["MS_CLIENT_ID"]
        )
        headers = validator(req.headers.get("authorization"))
    except TokenValidationError as e:
        return HttpResponse(status_code=401, body=f"TokenValidationError: {e}")
    
    # ingest the payload from the external event as a Pydantic object
    payload = HttpRequest.pydantize_body(req, ClientConfigPayload).model_dump()

    # set storage connection variables
    conn_str = (
        "MATCHBACK_CONN_STR"
        if "MATCHBACK_CONN_STR" in os.environ.keys()
        else "AzureWebJobsStorage"
    )
    egress = {
        "client_config_table":{"conn_str":conn_str, "table_name":"clientConfig"}
    }

    # read the client config cache to return previous columns mappings, if applicable
    table_client = TableClient.from_connection_string(
        conn_str=os.environ[egress['client_config_table']['conn_str']],
        table_name=egress['client_config_table']['table_name']
    )
    entities = table_client.query_entities(f"PartitionKey eq '{payload['group_id']}'")
    # extract timestamp property from entity metadata to be able to filter to the most recent
    entity_list = []
    for entity in entities:
        entity_list.append({
            **entity,
            "timestamp":entity._metadata["timestamp"]
        })
    df = pd.DataFrame(entity_list)
    # return the cached column mappings if they exist, otherwise return {}
    if len(df):
        most_recent_entity = df.sort_values('timestamp', ascending=False).iloc[0].to_dict()
        cached_data = {k:v for k,v in most_recent_entity.items() if k not in ['PartitionKey','RowKey','timestamp']}
    else:
        cached_data = {}

    return HttpResponse(
        json.dumps(cached_data),
        status_code=200
    )

    
class ClientConfigPayload(BaseModel):
   """
   Sales Uploader payload containing information on how to parse and process the sales file.
   """
   group_id:str
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationClient
import logging
import json
from azure.data.tables import TableClient
import os
from libs.utils.oauth2.tokens.microsoft import ValidateMicrosoft
from libs.utils.oauth2.tokens import TokenValidationError
from pydantic import BaseModel
import pandas as pd

bp = Blueprint()


@bp.route(route="esquire/salesUploader/readClientConfig", methods=["POST", "OPTIONS"])
@bp.durable_client_input(client_name="client")
async def http_salesUploader_readClientConfig(
    req: HttpRequest, client: DurableOrchestrationClient
):
    """
    POST request that returns sales column mappings of the previous file uploaded for a given client.

    Payload:
    group_id
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
        json.dumps(cached_data, indent=3),
        status_code=200
    )

    
class ClientConfigPayload(BaseModel):
   """
   Sales Uploader payload containing information on how to parse and process the sales file.
   """
   group_id:str
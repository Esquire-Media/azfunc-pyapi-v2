from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationClient
import os
import json
import pandas as pd
from azure.data.tables import TableClient
from libs.utils.oauth2.tokens.microsoft import ValidateMicrosoft
from libs.utils.oauth2.tokens import TokenValidationError
import logging

bp = Blueprint()

@bp.route(route="esquire/location_insights/getAssets", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def starter_locationInsights_getAssets(req: HttpRequest, client: DurableOrchestrationClient):

    # validate the MS bearer token to ensure the user is authorized to make requests
    logging.warning(req.headers.get('authorization'))
    try:
        validator = ValidateMicrosoft(
            tenant_id=os.environ['MS_TENANT_ID'], 
            client_id=os.environ['MS_CLIENT_ID']
        )
        headers = validator(req.headers.get('authorization'))
    except TokenValidationError as e:
        return HttpResponse(status_code=401, body=f"TokenValidationError: {e}")
    
    # if a campaign proposal conn string is set, use that. Otherwise use AzureWebJobsStorage
    conn_str = (
        "LOCATION_INSIGHTS_CONN_STR"
        if "LOCATION_INSIGHTS_CONN_STR" in os.environ.keys()
        else "AzureWebJobsStorage"
    )
    assets_table = { # table of valid asset package names
        "conn_str":conn_str,
        "table_name":"locationInsightsAssets"
    }

    # connect to the assets storage table
    table_client = TableClient.from_connection_string(conn_str=os.environ[assets_table["conn_str"]], table_name=assets_table["table_name"])
    assets = pd.DataFrame(table_client.list_entities())
    
    # build dictionary of assets by type
    assets_dict = {}
    for asset_type, df in assets.groupby('PartitionKey'):
        assets_dict[asset_type] = df['RowKey'].unique().tolist()

    return HttpResponse(
        json.dumps(assets_dict, indent=3),
        status_code=200
    )

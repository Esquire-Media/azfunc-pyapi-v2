from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationClient
import os
import json
import pandas as pd
from azure.data.tables import TableServiceClient

bp = Blueprint()

@bp.route(route="esquire/campaign_proposal/getAssets", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def starter_getAssets(req: HttpRequest, client: DurableOrchestrationClient):

    # connect to the assets storage table
    table_service = TableServiceClient.from_connection_string(conn_str = os.environ['AzureWebJobsStorage'])
    table_assets = table_service.get_table_client(table_name = "campaignproposalsassets")
    assets = pd.DataFrame(table_assets.list_entities())
    
    # build dictionary of assets by type
    assets_dict = {}
    for asset_type, df in assets.groupby('PartitionKey'):
        assets_dict[asset_type] = df['RowKey'].unique().tolist()

    return HttpResponse(
        json.dumps(assets_dict, indent=3),
        status_code=200
    )


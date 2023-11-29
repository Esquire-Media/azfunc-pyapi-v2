from libs.azure.functions import Blueprint
from azure.durable_functions import (
    DurableOrchestrationClient,
)
from libs.azure.functions.http import HttpRequest, HttpResponse
import logging

bp: Blueprint = Blueprint()


@bp.route(route="audiences/maids/test")
@bp.durable_client_input(client_name="client")
async def starter_esquireAudiencesMaid_daily(
    req: HttpRequest,
    client: DurableOrchestrationClient,
):
    # get audiences
    
#  file path:libs/azure/functions/blueprints/esquire/audiences/oneview/starters/test.py

from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest

bp = Blueprint()


@bp.route(route="audiences/oneview/test")
@bp.durable_client_input(client_name="client")
async def starter_esquireAudiencesOneView_test(
    req: HttpRequest,
    client: DurableOrchestrationClient,
):
    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="oneview_customaudience_orchestrator",
        client_input="clwjn2qeu005drw043l2lrnbv",
    )

    return client.create_check_status_response(req, instance_id)

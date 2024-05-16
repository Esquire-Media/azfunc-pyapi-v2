from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions.http import HttpRequest

bp = Blueprint()


@bp.route(route="audiences/meta/test")
@bp.durable_client_input(client_name="client")
async def starter_esquireAudiencesMeta_test(
    req: HttpRequest,
    client: DurableOrchestrationClient,
):
    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="meta_customaudience_orchestrator",
        client_input="clulpbfdg001v12jixniohdne"
    )

    return client.create_check_status_response(req, instance_id)
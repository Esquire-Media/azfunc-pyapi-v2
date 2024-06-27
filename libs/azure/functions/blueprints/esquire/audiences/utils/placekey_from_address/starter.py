from azure.durable_functions import Blueprint
from azure.functions import HttpRequest
from azure.durable_functions import DurableOrchestrationClient
import logging
from pydantic import BaseModel, conlist
from libs.utils.logging import AzureTableHandler
from libs.utils.pydantic.address import AddressComponents2
from datetime import timedelta

bp = Blueprint()

freshness_window = timedelta(days=365)

# initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("placekey.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)


@bp.route(route="esquire/placekeys/fromAddress", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def starter_placekeys_fromAddress(req: HttpRequest, client: DurableOrchestrationClient):
    """
    Endpoint for passing a list of address data objects and returning a list of placekeys.
    """
    payload = AddressPayload.model_validate_json(req.get_body()).model_dump()

    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_placekey_fromAddress",
        client_input=payload
    )

    return client.create_check_status_response(req, instance_id)

class AddressPayload(BaseModel):
    addresses: conlist(AddressComponents2, min_length=1)
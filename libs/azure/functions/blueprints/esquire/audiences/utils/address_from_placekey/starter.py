from azure.durable_functions import Blueprint
from azure.functions import HttpRequest
from azure.durable_functions import DurableOrchestrationClient
from pydantic import BaseModel, conlist
from datetime import timedelta
from libs.utils.pydantic.address import Placekey

bp = Blueprint()
freshness_window = timedelta(days=365)

@bp.route(route="esquire/addresses/fromPlacekey", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def starter_addresses_fromPlacekey(
    req: HttpRequest, client: DurableOrchestrationClient
):
    """
    Takes one or more placekeys, and returns data in the caching table associated with those placekeys.
    NOTE: The cached data is NOT a complete dataset - address info will not be returned if
        that address has never been the result of an address-to-placekey call previously.

    Input:
    {
        "placekeys":[
            "222@5xc-pdw-7h5",
            ...
        ]
    }

    Output:
    {
        "url":"..."
    }
    """

    # load the request payload as a Pydantic object
    payload = PlacekeyPayload.model_validate_json(req.get_body()).model_dump()

    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_addresses_fromPlacekey",
        client_input={
            "placekeys": list(set(payload["placekeys"]))
        },  # dedupe before sending to orchestrator
    )

    return client.create_check_status_response(req, instance_id)


@bp.route(route="esquire/addresses/csv/fromPlacekey", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def starter_addressCSV_fromPlacekey(
    req: HttpRequest, client: DurableOrchestrationClient
):
    """
    Takes one or more placekeys, and returns a CSV of address data from the caching table associated with those placekeys.
    NOTE: The cached data is NOT a complete dataset - address info will not be returned if
        that address has never been the result of an address-to-placekey call previously.

    Input:
    {
        "placekeys":[
            "222@5xc-pdw-7h5",
            ...
        ]
    }

    Output:
    {
        "url":"https://esquire..."
    }
    """

    # load the request payload as a Pydantic object
    payload = PlacekeyPayload.model_validate_json(req.get_body()).model_dump()

    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_addressCSV_fromPlacekey",
        client_input={
            "placekeys": list(set(payload["placekeys"])),
        },
    )

    return client.create_check_status_response(req, instance_id)

class PlacekeyPayload(BaseModel):
    placekeys: conlist(Placekey, min_length=1)
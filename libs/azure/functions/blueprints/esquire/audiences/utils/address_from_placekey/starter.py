from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationClient
import os
import json
import jwt
from azure.data.tables import TableClient
import hashlib
import logging
from pydantic import BaseModel, conlist
from libs.utils.logging import AzureTableHandler
from pydantic import validator
from libs.utils.oauth2.tokens.microsoft import ValidateMicrosoft
from libs.utils.oauth2.tokens import TokenValidationError
from libs.utils.pydantic.address import AddressComponents2
from datetime import datetime as dt, timedelta
from libs.utils.azure_storage import query_entities_to_list_of_dicts
from libs.utils.pydantic.address import Placekey
import requests

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
    payload = HttpRequest.pydantize_body(req, PlacekeyPayload).model_dump()

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
    payload = HttpRequest.pydantize_body(req, PlacekeyPayload).model_dump()

    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_addressCSV_fromPlacekey",
        client_input={
            "placekeys": list(set(payload["placekeys"])),
        },
    )

    return client.create_check_status_response(req, instance_id)

class PlacekeyPayload(BaseModel):
    placekeys: conlist(Placekey, min_length=1)
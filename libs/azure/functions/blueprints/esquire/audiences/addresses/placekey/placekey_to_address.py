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

# initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("placekey.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)



@bp.route(route="esquire/addresses/fromPlacekey", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def starter_addresses_fromPlacekey(req: HttpRequest, client: DurableOrchestrationClient):
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
    """
    logger = logging.getLogger("placekey.logger")

    # load the request payload as a Pydantic object
    payload = HttpRequest.pydantize_body(req, PlacekeyPayload).model_dump()
    deduped_placekeys = set(payload['placekeys'])

    # # validate the MS bearer token to ensure the user is authorized to make requests
    # try:
    #     validator = ValidateMicrosoft(
    #         tenant_id=os.environ['MS_TENANT_ID'], 
    #         client_id=os.environ['MS_CLIENT_ID']
    #     )
    #     headers = validator(req.headers.get('authorization'))
    # except TokenValidationError as e:
    #     return HttpResponse(status_code=401, body=f"TokenValidationError: {e}")
    
    # # extract user information from bearer token metadata
    # payload['user'] = headers['oid']

    logging.warning(payload)

    # connect to placekey cache table
    table = TableClient.from_connection_string(
        conn_str=os.environ["ADDRESSES_CONN_STR"], table_name="placekeys"
    )

    # find data for placekeys that already exist in the cache table
    entities_list = [
        table.query_entities(f"PartitionKey eq '{placekey}'")
        for placekey in deduped_placekeys
    ]
    # convert query entities into dicts for HTTP response
    list_of_lists = [
        query_entities_to_list_of_dicts(
            entities=entities,
            partition_name='placekey',
            row_name='md5'
        )
        for entities in entities_list
    ]
    res = []
    for sublist in list_of_lists:
        for dictionary in sublist:
            res.append(dictionary)

    return HttpResponse(
        json.dumps(res, indent=2),
        status_code=200
    )

class PlacekeyPayload(BaseModel):
    placekeys: conlist(str, min_length=1)
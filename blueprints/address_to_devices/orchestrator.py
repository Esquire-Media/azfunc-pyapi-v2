from azure.durable_functions import (
    DurableOrchestrationClient,
    DurableOrchestrationContext,
    RetryOptions,
)
from libs.azure.functions.http import HttpRequest
from libs.azure.functions import Blueprint
from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging
import json
import os
import uuid
from pydantic import BaseModel
from libs.utils.pydantic.address import AddressComponents2

# Create a Blueprint instance
bp = Blueprint()


class AddressToDevicesPayload(BaseModel):
    addresses: list[AddressComponents2]


@bp.route(route="address_to_device/starter", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def starter_addressToDevices(
    req: HttpRequest, client: DurableOrchestrationClient
):

    payload = HttpRequest.pydantize_body(req, AddressToDevicesPayload).model_dump()

    instance_id = await client.start_new(
        "orchestrator_addressToDevices",
        client_input={**payload},
    )

    return client.create_check_status_response(request=req, instance_id=instance_id)


@bp.orchestration_trigger(context_name="context")
def orchestrator_addressToDevices(context: DurableOrchestrationContext):
    """
    Params:

    blob url : of a file containing placekeys
    OR a list of placekeys

    Process:
    reach out to our placekey mappings database and convert back to addresses
    123
        123 Main St
        123 Main St Apt 1

    build an endpoint that takes in a placekey, and returns CSV data for that address / address set (Make sure private access)
    send that url to Onspot
    Onspot returns a one-column CSV of deviceids, and a debug.csv file (ignore this one)
    build a cache of deviceid/address mappings

    Outputs:

    Step 1:
    GET placekey for address
    GET address for placekey
    handle caching, staleness refresh, no-key error (AzTable for caching)
    BULK functionality
    """

    retry = RetryOptions(15000, 3)
    conn_str = "ADDRESSES_CONN_STR"
    container = "address-to-device"
    ingress = context.get_input()

    # convert each address to a placekey for unique indexing
    placekeys = yield context.call_sub_orchestrator(
        name="orchestrator_placekey_fromAddress",
        input_={"addresses": ingress["addresses"]},
    )

    # generate a CSV of address data for each placekey
    address_blob_urls = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "orchestrator_addressCSV_fromPlacekey",
                {"placekeys": [placekey]},
            )
            for placekey in placekeys
        ]
    )
    address_blob_urls = [obj["url"] for obj in address_blob_urls]
    logging.warning(address_blob_urls)

    # onspot = yield context.call_sub_orchestrator(
    #     "onspot_orchestrator",
    #     {
    #         "conn_str":conn_str,
    #         "container":container,
    #         "outputPath":"devices",
    #         "endpoint":"/save/addresses/all/devices",
    #         "request":{
    #             "hash": False,
    #             "name": uuid.uuid4().hex,
    #             "fileName": uuid.uuid4().hex,
    #             "fileFormat": {
    #                 "delimiter": ",",
    #                 "quoteEncapsulate": True,
    #             },
    #             "sources": [blob_url],
    #             "mappings": {
    #                 "street": ["formatted_street_address"],
    #                 "city": ["city"],
    #                 "state": ["state"],
    #                 "zip": ["zip_code"],
    #                 "zip4": ["zip_plus_four_code"],
    #             },
    #             "matchAcceptanceThreshold": 29.9,
    #         }
    #     }
    # )
    # logging.warning(onspot)

    logging.warning("All Tasks Finished!")
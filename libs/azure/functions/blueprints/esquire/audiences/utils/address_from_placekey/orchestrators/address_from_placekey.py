# File: libs/azure/functions/blueprints/esquire/audiences/mover_sync/orchestrators/orchestrator.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
from libs.utils.pydantic.address import Placekey
from pydantic import BaseModel, conlist
import logging, os

bp = Blueprint()
batch_size = int(os.environ.get("ADDRESS_FROM_PLACEKEY_BATCH_SIZE", 100000))

@bp.orchestration_trigger(context_name="context")
def orchestrator_addresses_fromPlacekey(context: DurableOrchestrationContext):
    retry = RetryOptions(15000, 1)
    logger = logging.getLogger("placekey.logger")

    # validate the ingress payload's format
    payload = PlacekeyPayload.model_validate(
        context.get_input()
    ).model_dump()

    # call placekey-to-address conversions with a defined batch size and collate the results
    address_lists = yield context.task_all(
        [
            context.call_activity_with_retry(
                name="activity_addresses_fromPlacekeyBatch",
                retry_options=retry,
                input_=payload['placekeys'][i : i + batch_size]
            )
            for i in range(0, len(payload['placekeys']), batch_size)
        ]
    )

    # flatten list of lists
    addresses = [
        key
        for keylist in address_lists
        for key in keylist
    ]

    return addresses

class PlacekeyPayload(BaseModel):
    placekeys: conlist(Placekey, min_length=1)
# File: libs/azure/functions/blueprints/esquire/audiences/mover_sync/orchestrators/orchestrator.py

from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import os
import json
import jwt
import pandas as pd
from azure.data.tables import TableClient
import hashlib
import logging
from pydantic import BaseModel, conlist
from typing import Optional
from libs.utils.logging import AzureTableHandler
from pydantic import validator
from libs.utils.oauth2.tokens.microsoft import ValidateMicrosoft
from libs.utils.oauth2.tokens import TokenValidationError
from libs.utils.pydantic.address import AddressComponents2
from datetime import datetime as dt, timedelta
import requests

bp = Blueprint()
batch_size = os.environ.get("PLACEKEY_FROM_ADDRESS_BATCH_SIZE", 1000)


@bp.orchestration_trigger(context_name="context")
def orchestrator_placekey_fromAddress(context: DurableOrchestrationContext):
    """
    Orchestrator for converting an address to a placekey, and then caching the results in a storage table.
    The ouput will return a numerical index corresponding to the order of the input addresses.

    Input:
    ```
    {
        "addresses":[
            {
                "street":"6013 MERIDIAN DR",
                "city":"BRYANT",
                "state":"AR",
                "zipcode":"72022"
            },
            ...
        ]
    }
    ```

    Output:
    ```
    [
        "0c2hlvuz5k@8f2-55m-nt9",
        None,
        ...
    ]
    ```
    """
    retry = RetryOptions(15000, 1)
    logger = logging.getLogger("placekey.logger")

    # validate the ingress payload's format
    payload = AddressPayload.model_validate(
        context.get_input()
    ).model_dump()

    # call address-to-placekey conversions with a defined batch size and collate the results
    placekey_lists = yield context.task_all(
        [
            context.call_activity_with_retry(
                name="activity_placekey_fromAddressBatch",
                retry_options=retry,
                input_=[address for address in payload['addresses'][i : i + batch_size]]
            )
            for i in range(0, len(payload['addresses']), batch_size)
        ]
    )
    # flatten list of lists
    placekeys = [
        key
        for keylist in placekey_lists
        for key in keylist
    ]

    return placekeys

class AddressPayload(BaseModel):
    addresses: conlist(AddressComponents2, min_length=1)
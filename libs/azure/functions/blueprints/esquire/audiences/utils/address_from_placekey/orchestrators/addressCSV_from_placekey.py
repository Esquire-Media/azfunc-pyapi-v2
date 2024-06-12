# File: libs/azure/functions/blueprints/esquire/audiences/mover_sync/orchestrators/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from azure.durable_functions import Blueprint
from pydantic import BaseModel, conlist
from libs.utils.pydantic.address import Placekey
from azure.storage.blob import BlobClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime as dt, timedelta
from libs.utils.azure_storage import get_blob_sas
import logging
import pandas as pd
import os
import uuid

bp = Blueprint()
batch_size = int(os.environ.get("ADDRESS_FROM_PLACEKEY_BATCH_SIZE", 100000))

@bp.orchestration_trigger(context_name="context")
def orchestrator_addressCSV_fromPlacekey(context: DurableOrchestrationContext):
    retry = RetryOptions(15000, 1)
    logger = logging.getLogger("placekey.logger")
    conn_str = (
        "ADDRESSES_CONN_STR"
        if "ADDRESSES_CONN_STR" in os.environ.keys()
        else "AzureWebJobsStorage"
    )
    output_location = {"container_name": "placekey-blobs", "blob_name": str(uuid.uuid4())}

    # validate the ingress payload's format
    payload = PlacekeyCSVPayload.model_validate(context.get_input()).model_dump()

    # call placekey-to-address conversions with a defined batch size and collate the results
    address_lists = yield context.task_all(
        [
            context.call_activity_with_retry(
                name="activity_addresses_fromPlacekeyBatch",
                retry_options=retry,
                input_=payload["placekeys"][i : i + batch_size],
            )
            for i in range(0, len(payload["placekeys"]), batch_size)
        ]
    )

    # flatten list of lists
    addresses = [key for keylist in address_lists for key in keylist]

    # output as CSV and build the URL
    df = pd.DataFrame(addresses)
    df = df[["street", "city", "state", "zipcode"]]
    blob = BlobClient.from_connection_string(
        conn_str=os.environ[conn_str],
        container_name=output_location["container_name"],
        blob_name=output_location["blob_name"],
    )
    blob.upload_blob(df.to_csv(index=False))

    # generate a SAS URL with a default 2 day expiry time
    return {
        "url": get_blob_sas(blob=blob, expiry=timedelta(days=2), prefix="https://")
    }


class PlacekeyCSVPayload(BaseModel):
    placekeys: conlist(Placekey, min_length=1)
 
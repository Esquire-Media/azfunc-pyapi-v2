from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions
from azure.data.tables import TableClient
from libs.utils.pydantic.time import Date
from pydantic import BaseModel
from typing import Optional
import logging, os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_salesUploader(context: DurableOrchestrationContext):
    """
    Orchestrates the end-to-end process of uploading, validating, and processing sales data within the Sales Uploader application.

    The orchestration involves:
    - Waiting for an event indicating the sales data upload completion.
    - Preprocessing the uploaded data to clean and standardize it.
    - Validating address information with SmartyStreets (optional).
    - Post-processing the data to merge validated addresses and finalize the dataset.
    - Caching the upload configuration for future reference.

    Parameters:
    - context (DurableOrchestrationContext): The context object provided by Azure Durable Functions, containing orchestration metadata and methods to interact with other functions and events.
    """

    logging.warning("Orchestrator Started")

    # function ingress
    ingress = context.get_input()
    timestamp = context.current_utc_datetime.isoformat()
    retry = RetryOptions(15000, 1)

    # catch the salesUploaded external event
    salesUploadedEvent = yield context.wait_for_external_event(name="salesUploaded")

    # ingest the payload from the external event as a Pydantic object
    salesUploadedPayload = SalesUploaderPayload.model_validate(
        salesUploadedEvent
    ).model_dump()

    # build function egress
    egress = {
        **ingress,
        **salesUploadedPayload,
        "instance_id": context.instance_id,
        "timestamp": timestamp,
    }

    # run pre-processing operations before Smarty validation
    preprocessed_blob = yield context.call_activity_with_retry(
        "activity_salesUploader_salesPreProcessing",
        retry,
        egress,
    )

    # slice only the address data and send it to an activity for Smarty validation
    validated_sas = yield context.call_activity_with_retry(
        "activity_smarty_validateAddresses",
        retry,
        {
            "source": preprocessed_blob,
            "column_mapping": {
                "street": "address",
                "city": "city",
                "state": "state",
                "zipcode": "zipcode",
            },
            "destination": {
                "conn_str": egress["runtime_container"]["conn_str"],
                "container_name": egress["runtime_container"]["container_name"],
                "blob_name": f"{context.instance_id}/03_validated",
            },
        },
    )

    # run post-processing operations after Smarty validation
    merged_blob = yield context.call_activity_with_retry(
        "activity_salesUploader_salesPostProcessing",
        retry,
        {**egress, "processed_blob_url": validated_sas},
    )

    # cache the client config info to an Azure data table
    table_client = TableClient.from_connection_string(
        conn_str=os.environ[egress["client_config_table"]["conn_str"]],
        table_name=egress["client_config_table"]["table_name"],
    )
    table_client.upsert_entity(
        entity={
            "PartitionKey": salesUploadedPayload["settings"]["group_id"],
            "RowKey": context.instance_id,
            **salesUploadedPayload["columns"],
            "date_first": merged_blob["date_first"],
            "date_last": merged_blob["date_last"],
        }
    )

    logging.warning("All processes completed!")
    return {}


class SalesUploaderColumns(BaseModel):
    """
    Object which details the column header mapping of a sales file.
    """

    address: str = None
    city: Optional[str] = None
    state: Optional[str] = None
    zipcode: Optional[str] = None
    date: Optional[str] = None
    saleAmount: Optional[str] = None
    saleLocation: Optional[str] = None
    productDescription: Optional[str] = None
    productBrand: Optional[str] = None
    customerType: Optional[str] = None


class SalesUploderSettings(BaseModel):
    """
    Object which details the config settings for a sales file, including its group association.
    """

    group_id: str
    matchback_name: str
    date_fill: Date = None


class SalesUploaderPayload(BaseModel):
    """
    Sales Uploader payload containing information on how to parse and process the sales file.
    """

    settings: SalesUploderSettings
    columns: SalesUploaderColumns

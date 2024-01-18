from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import os
import orjson as json
from libs.azure.functions.http import HttpRequest, HttpResponse
from azure.storage.blob import BlobClient
from azure.data.tables import TableClient
import logging
from pydantic import BaseModel, validator
from libs.utils.pydantic.time import Date
from typing import Optional
from libs.azure.storage.blob.sas import get_blob_download_url
from datetime import timedelta
from libs.utils.smarty import bulk_validate
import pandas as pd

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_matchback_uploadSales(context: DurableOrchestrationContext):

    logging.warning('Orchestrator Started') 
    settings = context.get_input()
    
    # catch the salesUploaded external event
    salesUploadedEvent = yield context.wait_for_external_event(name="salesUploaded")

    # ingest the payload from the external event as a Pydantic object
    salesUploadedPayload = SalesUploaderPayload.model_validate(salesUploadedEvent).model_dump()
    logging.warning(salesUploadedPayload)

    # connect to the ingress sales blob
    ingress_client = BlobClient.from_connection_string(
        conn_str=os.environ[settings['uploads_container']['conn_str']],
        container_name=settings['uploads_container']['container_name'],
        blob_name=context.instance_id
    )
    ingress_data = pd.read_csv(get_blob_download_url(blob_client=ingress_client, expiry=timedelta(minutes=10)))

    # fill date values if not set or null values exist
    if 'date_fill' in salesUploadedPayload['settings'].keys():
        if 'date' in salesUploadedPayload['columns'].keys():
            ingress_data[salesUploadedPayload['columns']['date']].fillna(salesUploadedPayload['settings']['date_fill'])
        else:
            ingress_data['date'] = salesUploadedPayload['settings']['date_fill']

    # slice to only include relevant columns, and rename to a standard column set
    ingress_data = ingress_data[[col for col in ingress_data.columns if col in salesUploadedPayload['columns'].values()]]
    ingress_data = ingress_data.rename(columns = {v:k for k,v in salesUploadedPayload['columns'].items()})

    # conduct address normalization via Smarty
    existing_columns = ingress_data.columns
    validated = bulk_validate(
        df = ingress_data,
        address_col="address" if "address" in ingress_data.columns else None,
        city_col="city" if "city" in ingress_data.columns else None,
        state_col="state" if "state" in ingress_data.columns else None,
        zip_col="zipcode" if "zipcode" in ingress_data.columns else None
    )
    merged = pd.merge(
        validated[['delivery_line_1','city_name','state_abbreviation','zipcode']],
        ingress_data.drop(columns=['address','city','state','zipcode']),
        right_index=True,
        left_index=True
    )

    # cache the client config info to an Azure data table
    table_client = TableClient.from_connection_string(
        conn_str=os.environ[settings['client_config_table']['conn_str']],
        table_name=settings['client_config_table']['table_name']
    )
    table_client.upsert_entity(
        entity={
            "PartitionKey":salesUploadedPayload['settings']['group_id'],
            "RowKey":salesUploadedPayload['settings']['date_fill'],
            **salesUploadedPayload['columns']
        }
    )

    return {}
    

class SalesUploaderColumns(BaseModel):
    """
    Object which details the column header mapping of a sales file.
    """
    address:str = None
    city:Optional[str] = None
    state:Optional[str] = None
    zipcode:Optional[str] = None
    date:Optional[str] = None
    saleAmount:Optional[str] = None
    saleLocation:Optional[str] = None
    productDescription:Optional[str] = None
    productBrand:Optional[str] = None
    customerType:Optional[str] = None

class SalesUploderSettings(BaseModel):
    """
    Object which details the config settings for a sales file, including its group association.
    """
    group_id:str
    date_fill:Date = None

class SalesUploaderPayload(BaseModel):
   """
   Sales Uploader payload containing information on how to parse and process the sales file.
   """

   settings:SalesUploderSettings
   columns:SalesUploaderColumns
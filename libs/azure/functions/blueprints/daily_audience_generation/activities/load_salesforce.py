from libs.azure.functions import Blueprint
import pandas as pd
import os
import json
from io import BytesIO
from azure.durable_functions import (
    DurableOrchestrationClient,
    DurableOrchestrationContext,
    EntityId,
    RetryOptions
)
from azure.storage.blob import BlobServiceClient
from libs.azure.functions.http import HttpRequest, HttpResponse
from sqlalchemy.orm import Session
from libs.data import from_bind
import geojson
import uuid

bp:Blueprint = Blueprint()

@bp.activity_trigger(input_name='ingress')
def activity_load_salesforce(ingress:dict):
    # connect to Synapse salesforce database
    provider = from_bind("salesforce")
    session: Session = provider.connect()

    # load models for each data object
    audience = provider.models["dbo"]["Audience__c"]
    child_id = provider.models["dbo"]["Child_ID__c"]
    account = provider.models["dbo"]["Account"]

    # query for the list of audience objects which are active and not deleted
    df = pd.DataFrame(session.query(
        audience.Id,
        audience.Audience_Name__c,
        audience.Audience_Type__c,
        audience.Lookback_Window__c,
    ).join(
        child_id,
        child_id.Id == audience.Child_ID__c,
    ).join(
        account,
        account.Id == child_id.Parent_Account__c,
    ).filter(
        audience.IsDeleted == False,
        audience.Refresh_Rate__c == None,
        child_id.IsDeleted == False,
        account.IsDeleted == False,
        account.Account_Status__c == "Active",
    ).all())

    # write salesforce data result to blob storage
    container_client = BlobServiceClient.from_connection_string(os.environ['AzureWebJobsStorage']).get_container_client('daily-audience-generation')
    blob_info = []

    # iterate by audience type and then ID
    for aud_type, audiences_by_type in df.groupby('Audience_Type__c'):
        for aud_id, audiences_by_id in audiences_by_type.groupby('Id'):            
            # upload to blob storage
            blob_name = f"{ingress['instance_id']}/{aud_type}/{aud_id}/info.csv"
            container_client.upload_blob(name=blob_name, data=df.to_csv(index=False), overwrite=True)
            blob_info.append({
                "blob_name":blob_name,
                "audience_type":aud_type
            })

    return blob_info
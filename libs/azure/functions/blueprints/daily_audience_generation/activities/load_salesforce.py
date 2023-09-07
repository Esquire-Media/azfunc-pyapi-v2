# File: libs/azure/functions/blueprints/daily_audience_generation/activities/load_salesforce.py

from libs.azure.functions import Blueprint
import pandas as pd
from sqlalchemy.orm import Session
from libs.data import from_bind

bp: Blueprint = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_load_salesforce(ingress: dict):
    # connect to Synapse salesforce database
    provider = from_bind("salesforce")
    session: Session = provider.connect()

    # load models for each data object
    audience = provider.models["dbo"]["Audience__c"]
    child_id = provider.models["dbo"]["Child_ID__c"]
    account = provider.models["dbo"]["Account"]

    # query for the list of audience objects which are active and not deleted
    df = pd.DataFrame(
        session.query(
            audience.Id,
            audience.Audience_Name__c,
            audience.Audience_Type__c,
            audience.Lookback_Window__c,
            child_id.Name, # Need to add to what gets returned
        )
        .join(
            child_id,
            child_id.Id == audience.Child_ID__c,
        )
        .join(
            account,
            account.Id == child_id.Parent_Account__c,
        )
        .filter(
            audience.IsDeleted == False,
            audience.Refresh_Rate__c == None,
            child_id.IsDeleted == False,
            account.IsDeleted == False,
            account.Account_Status__c == "Active",
        )
        .all()
    )

    # iterate by audience type and then ID
    return [
        {
            "Id": aud_id,
            "Audience_Name__c": str(audiences_by_id["Audience_Name__c"].values[0]),
            "Audience_Type__c": str(audiences_by_id["Audience_Type__c"].values[0]),
            "Lookback_Window__c": str(audiences_by_id["Lookback_Window__c"].values[0]),
            "ESQ_id": str(audiences_by_id["Name"].values[0]),
        }
        for aud_type, audiences_by_type in df.groupby("Audience_Type__c")
        for aud_id, audiences_by_id in audiences_by_type.groupby("Id")
    ]

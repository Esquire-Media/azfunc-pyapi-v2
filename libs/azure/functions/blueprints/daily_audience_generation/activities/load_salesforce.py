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
    return pd.DataFrame(
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
    ).to_dict(orient="records")
# File: example/blueprints/daily_dashboard_onspot/activities/geoframes.py

from libs.azure.functions import Blueprint
from libs.data import from_bind
from libs.data.structured.sqlalchemy import SQLAlchemyStructuredProvider
import geojson

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
async def daily_dashboard_onspot_activity_geoframes(ingress: dict):
    provider: SQLAlchemyStructuredProvider = from_bind("salesforce")
    tables = provider.models["dbo"]
    session = provider.connect()

    return [
        (
            row.Name,
            geojson.loads(
                row.JSON_String__c[:-1]
                if row.JSON_String__c[-1] == ","
                else row.JSON_String__c
            ),
        )
        for row in session.query(
            tables["GeoJSON_Location__c"].Name,
            tables["GeoJSON_Location__c"].JSON_String__c,
        )
        .join(
            tables["GeoJSON_Join__c"],
            tables["GeoJSON_Join__c"].GeoJSON_Location__c
            == tables["GeoJSON_Location__c"].Id,
        )
        .join(
            tables["Audience__c"],
            tables["Audience__c"].Id == tables["GeoJSON_Join__c"].Audience__c,
        )
        .join(
            tables["Child_ID__c"],
            tables["Child_ID__c"].Id == tables["Audience__c"].Child_ID__c,
        )
        .join(
            tables["Account"],
            tables["Account"].Id == tables["Child_ID__c"].Parent_Account__c,
        )
        .filter(
            tables["Account"].IsDeleted == False,
            tables["Child_ID__c"].IsDeleted == False,
            tables["Audience__c"].IsDeleted == False,
            tables["GeoJSON_Join__c"].IsDeleted == False,
            tables["GeoJSON_Location__c"].IsDeleted == False,
            tables["Account"].Account_Status__c == "Active",
            tables["Audience__c"].Active_Audience__c == True,
            tables["Audience__c"].Audience_Type__c == "Competitor Location",
            tables["GeoJSON_Join__c"].Active_Location__c == True,
        )
        .distinct()
    ]

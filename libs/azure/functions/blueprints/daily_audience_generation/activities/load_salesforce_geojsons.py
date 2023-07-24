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

# activity to fill in the geo data for each audience object
@bp.activity_trigger(input_name='ingress')
def activity_load_salesforce_geojsons(ingress:dict):

    # load competitor location geometries from salesforce
    audience_id = ingress['blob_name'].split('/')[-2]
    feature_collection = {
        "type":"FeatureCollection",
        "features":get_geojson(audience_id)
    }

    # upload each featurecollection to blob storage under the audienceID
    if len(feature_collection['features']):
        container_client = BlobServiceClient.from_connection_string(os.environ['AzureWebJobsStorage']).get_container_client('daily-audience-generation')
        blob_name = f"{'/'.join(ingress['blob_name'].split('/')[:-1])}/features.geojson"
        container_client.upload_blob(name=blob_name, data=json.dumps(feature_collection, indent=2), overwrite=True)

    return {}


def get_geojson(audienceID:str) -> list:
    provider = from_bind("salesforce")
    session: Session = provider.connect()

    geojoin = provider.models["dbo"]["GeoJSON_Join__c"]
    location = provider.models["dbo"]["GeoJSON_Location__c"]

    # list of audience objects -> not deleted and active.
    df = pd.DataFrame(
        session.query(location.JSON_String__c)
        .join(
            geojoin,
            location.Id == geojoin.GeoJSON_Location__c,
        )
        .filter(geojoin.Audience__c == audienceID)
        .all()
    )

    session.close()
    if "JSON_String__c" in df.columns:
        return list(
            map(
                lambda g: geojson.loads(g.strip()[:-1])
                if g.strip().endswith(",")
                else geojson.loads(g.strip()),
                df["JSON_String__c"].to_list(),
            )
        )
    return []
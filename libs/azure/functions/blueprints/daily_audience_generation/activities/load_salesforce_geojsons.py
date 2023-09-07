# File: libs/azure/functions/blueprints/daily_audience_generation/activities/load_salesforce_geojsons.py

from libs.azure.functions import Blueprint
import pandas as pd
import os, json, uuid
from azure.storage.blob import ContainerClient, ContainerSasPermissions, generate_container_sas
from sqlalchemy.orm import Session
from libs.data import from_bind
import geojson
from datetime import datetime
from dateutil.relativedelta import relativedelta

bp: Blueprint = Blueprint()


# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
def activity_load_salesforce_geojsons(ingress: dict):
    # load competitor location geometries from salesforce
    feature_collection = {
        "type": "FeatureCollection",
        "features": get_geojson(ingress["Id"]),
    }

    # upload each featurecollection to blob storage under the audienceID
    # Investigate here for lack of geojson SalesForce records
    if len(feature_collection["features"]):
        container_client = ContainerClient.from_connection_string(
            conn_str=os.environ["ONSPOT_CONN_STR"],
            container_name="general",
        )
        now = datetime.utcnow()
        end_time = datetime(now.year, now.month, now.day) - relativedelta(days=2)
        default_lookback = {
            "New Mover": relativedelta(months=3),
            "In Market Shoppers": relativedelta(months=6),
            "Digital Neighbors": relativedelta(months=4),
            "Competitor Location": relativedelta(days=75),
        }
        lookback = {
            "1 Month": relativedelta(months=1),
            "3 Months": relativedelta(months=3),
            "6 Months": relativedelta(months=6),
        }

        # put the start and end date per each polygon in the audience
        for feature in feature_collection["features"]:
            feature["properties"]["name"] = feature["properties"]["location_id"]
            feature["properties"]["start"] = (
                end_time
                - lookback.get(
                    ingress["Lookback_Window__c"][0],
                    default_lookback.get(
                        ingress["Audience_Type__c"][0],
                        relativedelta(days=60),
                    ),
                )
            ).isoformat()
            feature["properties"]["end"] = end_time.isoformat()
            feature["properties"]["hash"] = False
            
            # set output location
            sas_token = generate_container_sas(
                account_name=container_client.account_name,
                account_key=container_client.credential.account_key,
                container_name=container_client.container_name,
                permission=ContainerSasPermissions(write=True, read=True),
                expiry=datetime.utcnow() + relativedelta(days=2),
            )
            output_location = (
                container_client.url.replace("https://", "az://")
                + f"test/onspot/audiences/{ingress['Id']}?"
                + sas_token
            )
            feature["properties"]["outputLocation"] = output_location
            
            
        container_client.upload_blob(
            name=f"{ingress['blob_prefix']}/{ingress['instance_id']}/{ingress['Id']}.geojson",
            data=json.dumps(feature_collection),
            overwrite=True,
        )

    return {}


def get_geojson(audienceID: str) -> list:
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

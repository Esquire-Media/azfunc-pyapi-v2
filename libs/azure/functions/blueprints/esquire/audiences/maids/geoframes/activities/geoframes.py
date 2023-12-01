# File: libs/azure/functions/blueprints/esquire/audiences/maids/geoframes/activities/geoframes.py

from azure.storage.blob import BlobClient
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
from libs.data import from_bind
from sqlalchemy.orm import Session
import json, geojson, os, pandas as pd, logging

bp: Blueprint = Blueprint()


# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesMaidsGeoframes_geoframes(ingress: dict):
    audience = get_audience(ingress["audience"]["id"])
    # load competitor location geometries from salesforce
    feature_collection = {
        "type": "FeatureCollection",
        "features": get_geojson(ingress["audience"]["id"]),
    }

    # upload each featurecollection to blob storage under the audienceID
    # Investigate here for lack of geojson SalesForce records
    if len(feature_collection["features"]):
        now = datetime.utcnow()
        end_time = datetime(now.year, now.month, now.day) - relativedelta(days=2)
        default_lookback = {
            "InMarket Shoppers": relativedelta(months=6),
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
            feature["properties"]["fileName"] = "{}_{}".format(
                ingress["audience"]["id"],
                feature["properties"]["location_id"],
            )
            feature["properties"]["start"] = (
                end_time
                - lookback.get(
                    audience["lookback_window__c"],
                    default_lookback.get(
                        audience["audience_type__c"],
                        relativedelta(days=60),
                    ),
                )
            ).isoformat()
            feature["properties"]["end"] = end_time.isoformat()
            feature["properties"]["hash"] = False

    if ingress.get("destination"):
        # Configuring BlobClient for data upload
        if isinstance(ingress["destination"], str):
            blob = BlobClient.from_blob_url(ingress["destination"])
        elif isinstance(ingress["destination"], dict):
            blob = BlobClient.from_connection_string(
                conn_str=os.environ[ingress["destination"]["conn_str"]],
                container_name=ingress["destination"]["container_name"],
                blob_name=ingress["destination"]["blob_name"],
            )
        blob.upload_blob(
            data=json.dumps(feature_collection),
            overwrite=True,
        )
    else:
        return feature_collection


def get_audience(audience_id: str) -> dict:
    provider = from_bind("salesforce")
    qf = provider["dbo.Audience__c"]
    return qf[qf["Id"] == audience_id].to_pandas().to_dict(orient="records")[0]


def get_geojson(audience_id: str) -> list:
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
        .filter(geojoin.Audience__c == audience_id)
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

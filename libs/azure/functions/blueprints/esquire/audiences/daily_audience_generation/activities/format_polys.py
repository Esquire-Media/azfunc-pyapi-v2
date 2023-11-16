# File: libs/azure/functions/blueprints/esquire/audiences/daily_audience_generation/activities/format_polys.py

from libs.azure.functions import Blueprint
import os, json
from azure.storage.blob import (
    ContainerClient,
    ContainerSasPermissions,
    generate_container_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta

bp: Blueprint = Blueprint()


# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
def activity_dailyAudienceGeneration_formatPolys(ingress: dict):
    # load competitor location geometries from salesforce
    feature_collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": poly,
                "properties": {"audience_id": ingress["audience_info"]["Id"]},
            }
            for poly in ingress["polys"]
        ],
    }

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
            "Friends and Family": relativedelta(days=90),
        }
        lookback = {
            "1 Month": relativedelta(months=1),
            "3 Months": relativedelta(months=3),
            "6 Months": relativedelta(months=6),
        }

        # put the start and end date per each polygon in the audience
        for feature in feature_collection["features"]:
            feature["properties"]["name"] = ingress["audience_info"]["Audience_Name__c"]
            feature["properties"]["fileName"] = "{}_{}".format(
                ingress["audience_info"]["Audience_Name__c"],
                ingress["audience_info"]["Id"],
            )
            feature["properties"]["start"] = (
                end_time
                - lookback.get(
                    ingress["audience_info"]["Lookback_Window__c"],
                    default_lookback.get(
                        ingress["audience_info"]["Audience_Type__c"],
                        relativedelta(days=60),
                    ),
                )
            ).isoformat()
            feature["properties"]["end"] = end_time.isoformat()
            feature["properties"]["hash"] = False

            # set output location
            # sas_token = generate_container_sas(
            #     account_name=container_client.account_name,
            #     account_key=container_client.credential.account_key,
            #     container_name=container_client.container_name,
            #     permission=ContainerSasPermissions(write=True, read=True),
            #     expiry=datetime.utcnow() + relativedelta(days=2),
            # )
            # output_location = (
            #     container_client.url.replace("https://", "az://")
            #     + f"/audiences_test/{ingress['audience_info']['Id']}?"
            #     + sas_token
            # )
            # feature["properties"]["outputLocation"] = output_location
            # feature["properties"]["callback"] = "https://azfunc-jeami.tun.esqads.com"

        container_client.upload_blob(
            name=f"{ingress['blob_prefix']}/{ingress['audience_info']['Id']}/{ingress['audience_info']['Id']}.geojson",
            data=json.dumps(feature_collection),
            overwrite=True,
        )

    # if no features, return blank object
    return {}

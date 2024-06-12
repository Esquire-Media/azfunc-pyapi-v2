# File: libs/azure/functions/blueprints/esquire/audiences/maids/geoframes/activities/geoframes.py

from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy.orm import Session
import orjson as json, geojson, os, pandas as pd

bp: Blueprint = Blueprint()


# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesMaidsGeoframes_geoframes(ingress: dict):
    """
    Activity function to process and upload geoframes data for Esquire Audiences Maids.

    This function loads geojson data corresponding to a specified audience, formats it for OnSpot
    requests, and uploads the formatted data to Azure Blob Storage. If no destination is provided,
    the formatted geojson data is returned directly.

    Parameters
    ----------
    ingress : dict
        A dictionary containing the following keys:
        - audience: dict
            A dictionary containing the audience ID and other relevant details.
        - destination: str or dict, optional
            The destination for storing the formatted geojson data. Can be a URL string or a
            dictionary specifying Blob Storage details (including 'conn_str', 'container_name',
            'blob_name').

    Returns
    -------
    Union[str, dict]
        If a destination is provided, returns the SAS URL of the uploaded blob.
        Otherwise, returns the formatted geojson data.

    """

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
            data=json.dumps(feature_collection).decode(),
            overwrite=True,
        )

        return (
            blob.url
            + "?"
            + generate_blob_sas(
                account_name=blob.account_name,
                account_key=blob.credential.account_key,
                container_name=blob.container_name,
                blob_name=blob.blob_name,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + relativedelta(days=2),
            )
        )
    else:
        return feature_collection


def get_audience(audience_id: str) -> dict:
    """
    Fetches audience data from a database based on the given audience ID.

    Parameters
    ----------
    audience_id : str
        The ID of the audience for which data is to be fetched.

    Returns
    -------
    dict
        A dictionary containing audience data.
    """

    provider = from_bind("salesforce")
    qf = provider["dbo.Audience__c"]
    return qf[qf["Id"] == audience_id].to_pandas().to_dict(orient="records")[0]


def get_geojson(audience_id: str) -> list:
    """
    Retrieves geojson data for the given audience ID from a database.

    Parameters
    ----------
    audience_id : str
        The ID of the audience for which geojson data is to be fetched.

    Returns
    -------
    list
        A list of geojson objects corresponding to the audience ID.
    """

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

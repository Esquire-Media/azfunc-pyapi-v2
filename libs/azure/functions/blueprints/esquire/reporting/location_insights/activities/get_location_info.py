from azure.durable_functions import Blueprint
from azure.storage.blob import BlobClient
from libs.data import from_bind
from sqlalchemy.orm import Session
import os, pandas as pd, orjson

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()


# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_locationInsights_getLocationInfo(settings: dict):

    # connect to Locations table using a SQLAlchemy ORM session
    provider = from_bind("keystone")
    session: Session = provider.connect()

    # query for location info which matches the passed ESQ_ID(s)
    locations = pd.read_sql(
        f"""
            WITH centroids AS (
                SELECT
                    id,
                    ST_Centroid(ST_Collect(ST_GeomFromGeoJSON(feature->'geometry'))) AS centroid
                FROM 
                    keystone."TargetingGeoFrame",
                    jsonb_array_elements(polygon->'features') AS feature
                GROUP BY
                    id
            )
            SELECT
                G.id AS "ID",
                G."ESQID" AS "ESQ_ID",
                G.title AS "Owner",
                G.street AS "Address",
                G.city AS "City",
                G.state AS "State",
                G."zipCode" AS "Zip",
                G.polygon->'features'->0->'geometry' AS "Geometry",
                ST_Y(C.centroid) AS "Latitude",
                ST_X(C.centroid) AS "Longitude"
            FROM keystone."TargetingGeoFrame" AS G
            JOIN centroids AS C
                ON C.id = G.id
            WHERE
                G."ESQID" = '{settings["locationID"]}'
        """,
        session.connection()
    )
    locations['Geometry'] = locations['Geometry'].apply(lambda x: orjson.dumps(x).decode('utf-8'))
    
    # return the cleaned addresses as a list of component dictionaries, each with an index attribute
    if len(locations) == 0:
        raise Exception(
            f"Location with ESQ_ID '{settings['locationID']}' was not found in dbo.Locations"
        )
    blob_client: BlobClient = BlobClient.from_connection_string(
        conn_str=os.environ[settings["runtime_container"]["conn_str"]],
        container_name=settings["runtime_container"]["container_name"],
        blob_name=settings["runtime_container"]["location_blob"],
    )
    blob_client.upload_blob(data=locations.to_csv(index=False))

    # return the path to the locations.csv blob
    return {}

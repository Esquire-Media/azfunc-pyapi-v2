import os
from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
import pandas as pd
from azure.storage.blob import BlobClient

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()


# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_locationInsights_getLocationInfo(settings: dict):
    
    # connect to Locations table using a SQLAlchemy ORM session
    provider = from_bind("legacy")
    session: Session = provider.connect()
    locations = provider.models["dbo"]["Locations"]

    # query for location info which matches the passed ESQ_ID(s)
    locations = pd.DataFrame(
        session.query(
            locations.ID,
            locations.ESQ_ID,
            locations.Owner,
            text(
                """
                dbo.getStreetAddress(
                    [Street.Number]
                    ,[Street.Predirection]
                    ,[Street.Name]
                    ,[Street.Suffix]
                    ,[Street.Postdirection]
                    ,[Unit.Number]
                    ,[Unit.Type]
                ) AS [Address]
            """
            ),
            locations.City,
            locations.State,
            locations.Zip,
            text("dbo.geo2json([Geomask]) AS [Geometry]"),
            text("[LatLong].Lat AS [Latitude]"),
            text("[LatLong].Long AS [Longitude]"),
        )
        .filter(locations.ESQ_ID == settings["locationID"])
        .all(),
        columns=[
            "ID",
            "ESQ_ID",
            "Owner",
            "Address",
            "City",
            "State",
            "Zip",
            "Geometry",
            "Latitude",
            "Longitude",
        ],
    )

    # return the cleaned addresses as a list of component dictionaries, each with an index attribute
    if len(locations) == 0:
        raise Exception(f"Location with ESQ_ID '{settings['locationID']}' was not found in dbo.Locations")
    blob_client: BlobClient = BlobClient.from_connection_string(
        conn_str=os.environ[settings["runtime_container"]["conn_str"]],
        container_name=settings["runtime_container"]["container_name"],
        blob_name=settings["runtime_container"]["location_blob"],
    )
    blob_client.upload_blob(data=locations.to_csv(index=False))

    # return the path to the locations.csv blob
    return {}

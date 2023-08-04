# File: libs/azure/functions/blueprints/esquire/dashboard/onspot/activities/locations.py

from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
from libs.data import from_bind
from libs.data.structured.sqlalchemy import SQLAlchemyStructuredProvider
import os
import pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
async def esquire_dashboard_onspot_activity_locations(ingress: dict):
    provider: SQLAlchemyStructuredProvider = from_bind("universal")
    tables = provider.models["dbo"]
    session = provider.connect()

    df = pd.DataFrame(
        session.query(
            tables["Locations"].ID.label("location_id"),
            tables["Locations"].ESQ_ID.label("esq_id"),
        )
    )

    BlobClient.from_connection_string(
        os.environ[ingress["conn_str"]]
        if ingress.get("conn_str", None) in os.environ.keys()
        else os.environ["AzureWebJobsStorage"],
        ingress["container"],
        ingress["outputPath"],
    ).upload_blob(df.to_csv(index=None))

    return ""

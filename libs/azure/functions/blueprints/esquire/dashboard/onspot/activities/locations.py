# File: libs/azure/functions/blueprints/esquire/dashboard/onspot/activities/locations.py

from azure.durable_functions import Blueprint
from libs.data import from_bind
from libs.data.structured.sqlalchemy import SQLAlchemyStructuredProvider
from libs.utils.azure_storage import init_blob_client
import os, pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
async def esquire_dashboard_onspot_activity_locations(ingress: dict):
    provider: SQLAlchemyStructuredProvider = from_bind("keystone")
    tables = provider.models["keystone"]
    session = provider.connect()

    df = pd.DataFrame(
        session.query(
            tables["TargetingGeoFrame"].id.label("location_id"),
            tables["TargetingGeoFrame"].ESQID.label("esq_id"),
        )
    )

    init_blob_client(
        conn_str=os.environ[ingress["conn_str"]]
        if ingress.get("conn_str", None) in os.environ.keys()
        else os.environ["AzureWebJobsStorage"],
        container_name=ingress["container"],
        blob_name=ingress["outputPath"],
    ).upload_blob(df.to_csv(index=None))

    return ""

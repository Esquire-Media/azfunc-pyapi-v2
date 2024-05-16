#  file path:libs/azure/functions/blueprints/esquire/audiences/meta/activities/get_newest_audience.py

from azure.storage.blob.aio import ContainerClient
from datetime import datetime
from libs.azure.functions import Blueprint
import os

bp: Blueprint = Blueprint()

# activity to grab the geojson data and format the request files for OnSpot
@bp.activity_trigger(input_name="ingress")
async def activity_esquireAudiencesMeta_newestAudienceOld(ingress: dict):
    sources_container_client = ContainerClient.from_connection_string(
        conn_str=os.environ[ingress['conn_str']],
        container_name=ingress['container_name'],
    )
    if ingress['blob_prefix'][-1] != '/':
        ingress['blob_prefix'] += '/'
        
    async with sources_container_client:
        latest_date = datetime(1,1,1)
        latest_blob = None
        async for blob in sources_container_client.list_blobs(name_starts_with=ingress['blob_prefix']):
            parts = blob.name.split('/')
            if parts[3] == 'maids.csv':
                if (d := datetime.fromisoformat(parts[2])) > latest_date:
                    latest_date = d
                    latest_blob = blob.name
                    
    return str(latest_blob)
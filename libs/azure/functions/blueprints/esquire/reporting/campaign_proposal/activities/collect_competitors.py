import os
from azure.durable_functions import Blueprint
import pandas as pd
from azure.storage.blob import BlobServiceClient, ContainerClient
from io import BytesIO
from libs.utils.esquire.point_of_interest.poi_engine import POIEngine, recreate_POI_form
from libs.data import from_bind
from libs.utils.python import index_by_list
from libs.azure.key_vault import KeyVaultClient
from libs.azure.functions.blueprints.esquire.reporting.campaign_proposal.utility.competitor_map import map_competitors
from libs.utils.azure_storage import get_blob_sas
import re
import ast

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_campaignProposal_collectCompetitors(settings: dict):

    # import cleaned addresses from previous step
    container_client: ContainerClient = ContainerClient.from_connection_string(conn_str=os.environ[settings["runtime_container"]['conn_str']], container_name=settings["runtime_container"]["container_name"])
    in_client = container_client.get_blob_client(blob=f"{settings['instance_id']}/addresses.csv")
    addresses = pd.read_csv(get_blob_sas(in_client), usecols=['address','latitude','longitude'])

    # connect to key vault to get mapbox token
    mapbox_key_vault = KeyVaultClient('mapbox-service')
    mapbox_token = mapbox_key_vault.get_secret('mapbox-token').value

    # find nearby competitors in the passed categor(ies)
    engine = POIEngine(provider_poi=from_bind("foursquare"), provider_esq=from_bind("legacy"))
    for r in [10,15,20]:  # expand radius until at least 20 competitors are found, or radius reaches 20 miles
        points = [(addr['latitude'], addr['longitude']) for i, addr in addresses.iterrows()]
        # pull search area as a combined polygon
        comps = engine.load_from_points(
            points=points,
            radius=1609*r, # convert meters to miles
            categories=settings['categoryIDs']
        )
        if len(comps) > 20:
            break

    # filter out FSQ POIs that are less likely to be "real"
    comps['closed_bucket_value'] = index_by_list(comps['closed_bucket'], sort_list=['VeryLikelyClosed','LikelyClosed','Unsure','LikelyOpen','VeryLikelyOpen'])
    comps['venue_reality_bucket_value'] = index_by_list(comps['venue_reality_bucket'], sort_list=['VeryLow','Low','Medium','High','VeryHigh'])
    comps['reality_score'] = comps['closed_bucket_value'] + comps['venue_reality_bucket_value']
    comps = comps[comps['reality_score']>=6]
    # use chain name where applicable, and slice the returned columns
    comps['chain_name'] = comps.apply(
        lambda x:
        ast.literal_eval(x['fsq_chain_name'])[0] if x['fsq_chain_name'] is not None else x['name'],
        axis=1
    )
    # calculate distances between each source/comp pair, up to a specified radius
    distances = recreate_POI_form(sources=addresses, query_pool=comps, radius=r)
    distances = distances[['esq_id','fsq_id','chain_name','address','city','state','zipcode','source','distance_miles']]
    distances['esq_id'] = distances['esq_id'].replace('null','')
    # filter to unique competitors across all queries
    unique_comps = distances.drop_duplicates('fsq_id', keep='first')

    # EXPORT COMPETITORS LIST (for function use)
    out_client = container_client.get_blob_client(blob=f"{settings['instance_id']}/competitors.csv")
    out_client.upload_blob(comps.to_csv(index=False), overwrite=True)

    # EXPORT COMPETITORS FILE (for email export)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        unique = distances.sort_values('distance_miles', ascending=True).drop_duplicates(subset=['fsq_id'],keep='first')
        unique.to_excel(writer, sheet_name='Unique', index=False)
        for source, source_df in distances.groupby('source'):
            # format sheet name within Excel's accepted character set
            sheet_name = re.sub(pattern="[\<\>\*\\\/\?|]", repl='.', string=source[:31])
            # export each source to its own tab
            source_df = source_df.sort_values('distance_miles', ascending=True).drop_duplicates(subset=['fsq_id'],keep='first')
            source_df.to_excel(writer, sheet_name=sheet_name, index=False)
            
    out_client = container_client.get_blob_client(blob=f"{settings['instance_id']}/Competitors-{settings['name']}.xlsx")
    out_client.upload_blob(output.getvalue(), overwrite=True)

    # COMPETITOR SCATTERMAPS
    bytes = map_competitors(
        comps=comps,
        owned=addresses,
        mapbox_token=mapbox_token,
        return_bytes=True
    )
    out_client = container_client.get_blob_client(blob=f"{settings['instance_id']}/competitors.png")
    out_client.upload_blob(bytes, overwrite=True)
    
    return {}
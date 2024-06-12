from azure.durable_functions import Blueprint
from azure.storage.blob import ContainerClient
from datetime import datetime as dt, timedelta
from libs.azure.functions.blueprints.esquire.reporting.campaign_proposal.utility.zipcode_map import map_zipcodes
from libs.utils.esquire.movers.mover_engine import MoverEngine
from libs.utils.esquire.zipcodes.zipcode_engine import ZipcodeEngine
from libs.azure.key_vault import KeyVaultClient
from libs.data import from_bind
from libs.utils.azure_storage import get_blob_sas
import orjson as json, numpy as np, os, pandas as pd

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_campaignProposal_collectMovers(settings: dict):
    
    # import cleaned addresses from previous step
    container_client: ContainerClient = ContainerClient.from_connection_string(conn_str=os.environ[settings["runtime_container"]['conn_str']], container_name=settings["runtime_container"]["container_name"])
    in_client = container_client.get_blob_client(blob=f"{settings['instance_id']}/addresses.csv")
    addresses = pd.read_csv(get_blob_sas(in_client), usecols=['address','latitude','longitude'])

    # connect to key vault to get mapbox token
    mapbox_key_vault = KeyVaultClient('mapbox-service')
    mapbox_token = mapbox_key_vault.get_secret('mapbox-token').value

    # calculate date range to pull movers
    end_date = dt.today()
    num_days = 90
    start_date = end_date - timedelta(days=num_days)

    # initialize objects needed for the mover data collection
    address_to_zipcodes = []
    unique_zipcodes_list = []
    radii = settings['moverRadii']
    zipcode_engine = ZipcodeEngine(from_bind("legacy"))
    mover_engine = MoverEngine(from_bind("audiences"))

    # INDIVIDUAL LOCATION COUNTS
    # on the first run-through, we grab all the unique zipcodes and map each addr/radius with its associated zipcodes 
    for i, addr in addresses.iterrows():
        # print(addr['address'])
        addr_dict = {
            **addr
        }

        # mapping between each addr/radius pair and the zipcodes within it
        mapping_dict = {
            "addr_index":i,
        }

        for radius in radii:
            # print('\t',radius)
            # get zipcodes in radius around store latlong
            zip_radius_list = zipcode_engine.load_from_radius(
                latitude=addr["latitude"], longitude=addr["longitude"], radius=1609 * radius
            )
            zips = pd.DataFrame(zip_radius_list)
            if not len(zips):
                raise Exception(f"Error: No zipcodes found within {radius} miles of the address at index {i}.")
            zips["GeoJSON"] = zips["GeoJSON"].apply(json.loads)

            # store the unique zipcode geometries (for mapping purposes later)
            for i, z in zips.iterrows():
                if z['Zipcode'] not in [item['Zipcode'] for item in unique_zipcodes_list]:
                    unique_zipcodes_list.append({
                        'Zipcode':z['Zipcode'],
                        'GeoJSON':z['GeoJSON']  
                    })
            
            # update the add/radius -> zipcodes mapping
            mapping_dict[f"zips_{radius}"] = zips['Zipcode'].unique()    
        address_to_zipcodes.append(mapping_dict)

    # DataFrame of all unique zipcodes and their corresponding geometry
    unique_zipcodes = pd.DataFrame(unique_zipcodes_list)

    # get mover counts for each zipcode
    zipcode_mover_counts = dict(mover_engine.load_from_zipcodes(
        start_date=start_date,
        end_date=end_date,
        zipcodes=unique_zipcodes['Zipcode'],
        counts=True
    ))

    # calculate mover count for each addr/radius pair
    mapping = pd.DataFrame(address_to_zipcodes)
    for radius in radii:
        mapping[f'movers_{radius}mi'] = mapping[f'zips_{radius}'].apply(
            lambda zip_list:
            sum([zipcode_mover_counts[z] for z in zip_list if z in zipcode_mover_counts.keys()])
        )
    # collect mover counts into a table
    mover_counts = pd.merge(
        addresses,
        mapping[['addr_index', *[f'movers_{radius}mi' for radius in radii]]],
        left_index=True,
        right_on='addr_index'
    ).drop(columns='addr_index')

    # TOTAL MOVER COUNTS BY RADIUS
    total_mover_counts = {}
    for radius in radii:
        # find unique zipcodes and mover counts in the given radius
        radius_unique_zips = np.unique(np.hstack(mapping[f'zips_{radius}']))
        radius_unique_total = sum([zipcode_mover_counts[z] for z in radius_unique_zips if z in zipcode_mover_counts.keys()])
        total_mover_counts[f"movers_{radius}mi"] = radius_unique_total

        # create a zipcode map for this radius
        bytes = map_zipcodes(
            zips = unique_zipcodes[unique_zipcodes["Zipcode"].isin(radius_unique_zips)].reset_index(),
            mapbox_token=mapbox_token,
            return_bytes=True
        )
        # export zipcode maps
        out_client = container_client.get_blob_client(blob=f"{settings['instance_id']}/mover_map_{radius}mi.png")
        out_client.upload_blob(bytes, overwrite=True)

    # export mover counts file
    out_client = container_client.get_blob_client(blob=f"{settings['instance_id']}/mover_counts.csv")
    out_client.upload_blob(mover_counts.to_csv(), overwrite=True)

    # export mover totals file    
    out_client = container_client.get_blob_client(blob=f"{settings['instance_id']}/mover_totals.csv")
    out_client.upload_blob(pd.DataFrame([total_mover_counts]).to_csv(), overwrite=True)

    return {}
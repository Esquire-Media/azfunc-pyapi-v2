import os
from libs.azure.functions import Blueprint
import pandas as pd
from libs.utils.smarty import bulk_validate
from azure.storage.blob import BlobServiceClient, ContainerClient
from io import BytesIO
from datetime import datetime as dt, timedelta
from libs.azure.functions.blueprints.esquire.campaign_proposal.utility.zipcode_map import map_zipcodes
from libs.utils.esquire.movers.mover_engine import MoverEngine
# from libs.azure.functions.blueprints.esquire.zipcodes.zipcode_engine import ZipcodeEngine
from libs.data import from_bind


# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_campaignProposal_collectMovers(settings: dict):
    
    # import cleaned addresses from previous step
    container_client: ContainerClient = BlobServiceClient.from_connection_string(os.environ["ESQUIRE_REPORTS_CONN_STR"]).get_container_client(container="campaign-proposals")
    in_client = container_client.get_blob_client(blob=f"{settings['instance_id']}/addresses.csv")
    addresses = pd.read_csv(BytesIO(in_client.download_blob().content_as_bytes()), usecols=['address','latitude','longitude'])

    # calculate date range to pull movers
    end_date = dt.today()
    num_days = 90
    start_date = end_date - timedelta(days=num_days)

    # initialize objects needed for the mover data collection
    mover_counts_list = []
    unique_zipcodes_list = []
    radii = settings['moverRadii']
    # zipcode_engine = ZipcodeEngine()
    mover_engine = MoverEngine(from_bind("audiences"))

    # # INDIVIDUAL LOCATION COUNTS
    # # execute mover data collection for each location and a deduped total
    # for i, addr in addresses.iterrows():
    #     addr_dict = {
    #         **addr
    #     }

    #     for radius in radii:
    #         # get zipcodes in radius around store latlong
    #         radius_list = zipcode_engine.get_radius_list(
    #             lat=addr["latitude"], lon=addr["longitude"], radius=1609 * radius
    #         )
    #         zips = pd.DataFrame(radius_list)
    #         zips["Radius"] = radius
    #         unique_zipcodes_list.append(zips)

    #         # get mover counts in the selected zipcodes
    #         mover_count = mover_engine.load_from_zipcodes(
    #             start_date=start_date,
    #             end_date=end_date,
    #             zipcodes=zips["Zipcode"].unique(),
    #             counts=True,
    #         ).iloc[0]["count"]
            
    #         addr_dict[f"movers_{radius}mi"] = mover_count
            
    #     mover_counts_list.append(addr_dict)

    # out_client = container_client.get_blob_client(blob=f"{settings['instance_id']}/mover_counts.csv")
    # out_client.upload_blob(pd.DataFrame(mover_counts_list).to_csv(), overwrite=True)

    # # TOTAL DEDUPED COUNTS
    # # collect unique zipcodes across all locations by shortest radius
    # unique_zipcodes = pd.concat(unique_zipcodes_list)
    # unique_zipcodes = unique_zipcodes.sort_values('Radius', ascending=True).drop_duplicates(subset=['Zipcode'], keep='first')
    
    # # get total deduped counts for each radius size
    # total_mover_counts = {}
    # for radius in radii:
    #     # pull total unique movers from all zipcodes
    #     unique_count = mover_engine.load_from_zipcodes(
    #         start_date=start_date,
    #         end_date=end_date,
    #         zipcodes=unique_zipcodes[unique_zipcodes['Radius']<=radius]["Zipcode"].unique(),
    #         counts=True,
    #     )
    #     total_mover_counts[f"movers_{radius}mi"] = int(unique_count.iloc[0]["count"])

    # out_client = container_client.get_blob_client(blob=f"{settings['instance_id']}/mover_totals.csv")
    # out_client.upload_blob(pd.DataFrame([total_mover_counts]).to_csv(), overwrite=True)

    # # ZIPCODE MAPS
    # for radius in radii:
    #     bytes = map_zipcodes(
    #         unique_zipcodes[unique_zipcodes["Radius"] <= radius].reset_index(),
    #         return_bytes=True
    #     )
    #     out_client = container_client.get_blob_client(blob=f"{settings['instance_id']}/mover_map_{radius}mi.png")
    #     out_client.upload_blob(bytes, overwrite=True)

    return {}

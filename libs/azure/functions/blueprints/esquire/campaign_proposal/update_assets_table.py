from azure.functions import TimerRequest
from libs.azure.functions import Blueprint
import os
from azure.data.tables import TableServiceClient
from azure.storage.blob import BlobServiceClient

bp = Blueprint()

@bp.timer_trigger(arg_name="timer", schedule="0 */30 * * * *")
def timer_UpdateAssetsTable(timer: TimerRequest):
    """
    Updates the Storage Table "campaignproposalsassets" with the names of each asset package currently in Blob Storage.
    This table will be the quick-access way to query the valid templates, creative_sets, and any other dynamic asset packages.
    Runs every 30 minutes.
    """
    
    # connect to blob storage client
    bsc = BlobServiceClient.from_connection_string(os.environ['AzureWebJobsStorage'])
    cc = bsc.get_container_client('campaign-proposals-resources')

    # get unique resource packages in each of the pptx-resource directories
    templates = list(set([name.split('/')[1].split('.')[0] for name in cc.list_blob_names(name_starts_with='templates/')]))
    creative_sets = list(set([name.split('/')[1] for name in cc.list_blob_names(name_starts_with='creatives/')]))

    # connect to storage table which will contain the name of each asset package
    table_service = TableServiceClient.from_connection_string(conn_str = os.environ['AzureWebJobsStorage'])
    table_assets = table_service.get_table_client(table_name = "campaignproposalsassets")

    # add template names
    for template in templates:
        table_assets.upsert_entity(entity={'PartitionKey':'template','RowKey':template})
    # add creative set names
    for creative_set in creative_sets:
        table_assets.upsert_entity(entity={'PartitionKey':'creativeSet','RowKey':creative_set})
    # add promotion set names

    # remove assets that no longer exist
    for row in table_assets.list_entities():
        if row['RowKey'] not in templates + creative_sets:
            table_assets.delete_entity(row)
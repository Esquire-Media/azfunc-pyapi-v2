import logging
import os
import re
import json
import pandas as pd
from libs.azure.functions import Blueprint
from azure.storage.blob import ContainerClient
from azure.data.tables import TableClient
from datetime import datetime as dt, timedelta

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_moversSync_getMissingChunks(settings: dict):

    logging.warning('started activity_moversSync_getMissingChunks')

    # connect to container and table clients
    container_client = ContainerClient.from_connection_string(conn_str=os.environ[settings['runtime_container']['conn_str']], container_name=settings['runtime_container']['container_name'])
    table_client = TableClient.from_connection_string(conn_str=os.environ[settings['rowCounts_table']['conn_str']], table_name=settings['rowCounts_table']['table_name'])

    # read from the row counts table
    row_counts = pd.DataFrame(table_client.list_entities())
    row_counts['chunk_blob_type'] = row_counts['PartitionKey'].apply(lambda x: f"{x}-geocoded")
    # only enforce address validation on the most recent 24 weeks (~6 months) of data
    row_counts['Date'] = row_counts['RowKey'].apply(lambda x: re.search('[0-9]{4}_[0-9]{2}_[0-9]{2}', x)[0]).apply(lambda x: dt.strptime(x,'%Y_%m_%d'))
    row_counts[row_counts['Date']>=dt.today() - timedelta(weeks=24)]

    # collect a DataFrame of the existing address-validated chunks
    blobs = list(
        [*container_client.list_blobs(name_starts_with='movers-geocoded')] + \
        [*container_client.list_blobs(name_starts_with='premovers-geocoded')]
    )
    chunk_blobs = pd.DataFrame([{k:v for k,v in blob.items() if k in['name','container']} for blob in blobs])
    chunk_blobs = chunk_blobs[chunk_blobs['name'].str.contains('offset')]
    chunk_blobs['blob_type'] = chunk_blobs['name'].apply(lambda x: x.split('/')[0])
    chunk_blobs['blob_name'] = chunk_blobs['name'].apply(lambda x: x.split('/')[1])
    chunk_blobs['chunk_name'] = chunk_blobs['name'].apply(lambda x: x.split('/')[2])
    chunk_blobs = chunk_blobs.drop(columns=['name'])
    chunk_blobs.sort_values('blob_name')

    # merge the existing blobs against the expected blobs
    merged = pd.merge(
        row_counts,
        chunk_blobs,
        left_on=['chunk_blob_type','RowKey'],
        right_on=['blob_type','blob_name'],
        how="left"
    )

    # iterate through the blobs to identify the existing range(s) of data
    missing_data_list = []
    for blob_keys, blob_df in merged.groupby(['PartitionKey','RowKey']):
        row_count = blob_df['RowCount'].iloc[0]
        total_range = (0,row_count)

        existing_data_ranges = []
        # find existing data ranges, if applicable
        for i, row in blob_df[~blob_df['chunk_name'].isnull()].iterrows():
            capture_groups = re.search(pattern="offset=(\d+),limit=(\d+)",string=row['chunk_name'])
            offset = int(capture_groups.group(1))
            limit = int(capture_groups.group(2))

            existing_data_ranges.append((offset, offset+limit-1))

        # compare existing values to the total values to identify missing ranges of data that need to be pulled
        missing_ranges = get_missing_ranges(existing_data_ranges, total_range)
        adjusted_missing_ranges = adjust_range_length(missing_ranges, max_range_length=int(os.environ["ADDRESS_VALIDATION_CHUNK_SIZE"]))
        # return blob information and offset/limit defining the range of data to run
        for r in adjusted_missing_ranges:
            missing_data_list.append({
                "blob_type":blob_keys[0],
                "blob_name":blob_keys[1],
                "offset":int(r[0]),             # convert int64 to int
                "limit":int(r[1] - r[0] + 1)    # convert int64 to int
            })

    logging.warning(missing_data_list)
    return missing_data_list


def get_missing_ranges(existing_data, total_range):
    missing_ranges = []

    start, end = total_range
    current = start

    for existing_start, existing_end in existing_data:
        # Check if there is a gap between the current position and the start of the existing range
        if current < existing_start:
            missing_ranges.append((current, existing_start - 1))
        
        # Move the current position to the end of the existing range + 1
        current = existing_end + 1
    
    # Check if there is a gap between the last existing range and the end of the total range
    if current <= end:
        missing_ranges.append((current, end))

    return missing_ranges

def adjust_range_length(ranges, max_range_length=2):
    adjusted_ranges = []

    for start, end in ranges:
        current = start
        while current <= end:
            adjusted_end = min(current + max_range_length - 1, end)
            adjusted_ranges.append((current, adjusted_end))
            current = adjusted_end + 1

    return adjusted_ranges
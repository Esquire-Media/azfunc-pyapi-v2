import os
from libs.azure.functions import Blueprint
import numpy as np
import pandas as pd
from azure.storage.blob import BlobClient
from sklearn.neighbors import BallTree
from libs.utils.azure_storage import get_blob_sas, export_dataframe
from datetime import timedelta
from azure.storage.blob import BlobClient
import logging

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="ingress")
def activity_pixelPush_calculateStoreDistances(ingress: dict):
    """
    Processes user data to calculate the nearest store for each user and the distance to it, 
    leveraging a BallTree for efficient spatial queries. The user data and store locations 
    are loaded from specified sources, and the results are optionally uploaded to a specified 
    destination.

    This function requires a specific structure for the `ingress` parameter, which includes 
    sources for user data and store locations, and may include a destination for the results. 

    Parameters:
    - ingress (dict): A dictionary containing the following keys:
        - "source" (str): The blob SAS URL for the source user data.
        - "store_locations_source" (dict): A dictionary specifying the connection string, 
          container name, and blob name for the store locations data.
        - "destination" (optional, str or dict): The destination for the output data, 
          which can be a blob URL as a string or a dictionary specifying the connection string, 
          container name, blob name, and optional format for the output data.

    Returns:
    - str: If a destination is specified, returns the URL to the uploaded results blob, 
           including a SAS token with read permission. If no destination is provided, 
           the function does not return a URL.

    Example of `ingress` parameter:
    {
        "source": "path/to/user_data.csv",
        "store_locations_source": {
            "conn_str": "AZURE_STORAGE_CONNECTION_STRING",
            "container_name": "container-name",
            "blob_name": "store_locations.csv"
        },
        "destination": {
            "conn_str": "AZURE_STORAGE_CONNECTION_STRING",
            "container_name": "results-container",
            "blob_name": "results.csv",
            "format": "csv"
        }
    }
    """

    # load the pixel data
    users = pd.read_csv(ingress["source"])

    # load the client's store address data
    stores = pd.read_csv(get_blob_sas(
        blob=BlobClient.from_connection_string(
            conn_str=os.environ[ingress['store_locations_source']['conn_str']],
            container_name=ingress['store_locations_source']['container_name'],
            blob_name=ingress['store_locations_source']['blob_name']
        ),
        expiry=timedelta(hours=2)
    ))

    # build a BallTree for fast neighbor-checking
    user_coords = users[['latitude','longitude']].values
    store_coords = stores[['latitude','longitude']].values
    tree = BallTree(np.radians(store_coords), metric='haversine')

    # query the BallTree and calculate the nearest store to each user
    distances, indices = tree.query(np.radians(user_coords), k=1)
    distances_miles = distances * 3956
    nearest_stores = stores.iloc[indices.flatten()]

    # concat the results back onto the original dataset and prepare the result DataFrame
    results_df = pd.concat([
        users.reset_index(drop=True), 
        nearest_stores.reset_index(drop=True), 
        pd.DataFrame(distances_miles, columns=['distance'])
    ], axis=1)

    # format the output
    results_df['distance'] = results_df['distance'].apply(lambda x: round(x, 1))
    results_df = results_df.sort_values(by=['last_name','first_name','personal_email'])
    results_df = results_df.drop(columns=['latitude','longitude','address','address2','city','state','zip'])
    results_df = results_df.rename(columns={
        "delivery_line_1":"personal_street",
        "city_name":"personal_city",
        "state_abbreviation":"personal_state",
        "zipcode":"personal_zipcode",
        "store_name":"nearest_store_name",
        "store_number":"nearest_store_number",
        "distance":"nearest_store_distance_miles"
    })

    export_url = export_dataframe(df=results_df, destination=ingress["destination"])
    return export_url
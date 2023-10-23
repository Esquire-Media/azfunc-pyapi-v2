# import pandas as pd
# import numpy as np
# import os
# # from sklearn.neighbors import BallTree
# from fuzzywuzzy import fuzz
# import re
# import h3

# # from libs.azure.sql import AzSQLEngine
# from libs.utils.smarty import bulk_validate

# import plotly.graph_objects as go
# import plotly.express as px

# from libs.azure.functions import Blueprint
# from azure.storage.blob import (
#     ContainerClient,
#     ContainerSasPermissions,
#     generate_container_sas,
# )
# import os, logging
# from datetime import datetime
# from dateutil.relativedelta import relativedelta

# bp: Blueprint = Blueprint()


# @bp.activity_trigger(input_name="ingress")
# def activity_load_neighbor_addresses(ingress: dict):
#     # create df of the file's information
#     df = pd.read_csv(ingress['filename'], dtype=str)
#     df = df.dropna(how='all', axis=1)
#     # get only the columns we need for verification
#     df = detect_column_names(df)
#     df = df.fillna('')
    
#     # use Smarty to validate the source address 
#     ## (is this something we need to do in other spots?? We don't validate addresses elsewhere)
#     ## file format ['address','city','state','zipcode','plus4Code']
#     validated = bulk_validate(df, address_col='street',city_col='city',state_col='state',zip_col='zipcode')
#     validated = validated[['delivery_line_1','city_name','state_abbreviation','zipcode','latitude','longitude']]

#     # Append H3 column
#     validated["h3_index"] = validated.apply(
#         lambda row: h3.geo_to_h3(
#             lat=row["latitude"], lng=row["longitude"], resolution=5
#         ),
#         axis=1,
#     )


#     # query the db for same address within the h3 index(es)
#     query = f"""
#         SELECT
#             [address], [city], [state], [zipCode], [plus4Code], [latitude], [longitude]
#         FROM dbo.addresses
#         WHERE h3_index IN ({', '.join(["'"+idx+"'" for idx in validated['h3_index'].unique()])})
#         AND [address] <> ''
#         AND [city] <> ''
#         AND [state] <> ''
#         AND [zipCode] <> ''
#         AND [plus4Code] <> ''
#         AND [latitude] IS NOT NULL
#         AND [longitude] IS NOT NULL
#         ;"""

#     # send the query to get potential neighbors matches from dbo.addresses in Synapse
#     # connection to db
#     conn = "connection string"
#     potentials = pd.read_sql(query, conn)

#     # filter out null or empty-string entries for latitude and longitude
#     potentials['latitude'] = pd.to_numeric(potentials['latitude'], errors='coerce')
#     potentials['longitude'] = pd.to_numeric(potentials['longitude'], errors='coerce')
#     potentials = potentials.dropna(subset=['latitude'])
#     potentials = potentials.dropna(subset=['longitude'])
    
#     return {}


# def detect_column_names(df):
#     """
#     Attempt to automatically detect the address component columns in a sales or address file.encode
#     Returns a slice of the sales data with detected columns for [address, city, state, zipcode]
#     """
#     names = df.columns

#     # dictionary of common column headers for address components
#     mapping = {
#         'street':['address','street','delivery_line_1','line1','add'],
#         'city':['city'],
#         'state':['state','st','state_abbreviation'],
#         'zipcode':['zip','zipcode','postal','postalcodeid']
#     }

#     # find best fit for each address field
#     for dropdown, defaults in mapping.items():
#         column_scores = [
#             max([fuzz.ratio(column.upper(), default.upper()) for default in defaults])
#             for column in df.columns
#         ]
#         best_fit_idx = column_scores.index(max(column_scores))
#         best_fit = df.columns[best_fit_idx]
#         df = df.rename(columns={best_fit:dropdown})

#     return df[['street','city','state','zipcode']]

# def find_neighbors(targets, potentials, radius=500, max_matches=200):
#     """
#     Returns all neighbors within one radius (meters) of any address in a dataframe using a BallTree algorithm
    
#     radius : Radius to search around each address

#     max_matches : The maximum number of neighbors to pull for a single address
#     """

#     print('Calculating Neighbors...\n')
    
#     # find nearest neighbors using a haversine-formula BallTree algorithm
#     tree = BallTree(np.deg2rad(potentials[['latitude', 'longitude']].values), metric='haversine')
#     distances, indices = tree.query(np.deg2rad(np.c_[targets['latitude'], targets['longitude']]), k = max_matches)

#     indices_col = []
#     distances_col = []
#     r_m = 6371000  # conversion factor to meters

#     # get all indices of neighbors
#     for indice_list in indices:
#         for ind in indice_list:
#             indices_col.append(ind)

#     # get all distances of neighbors
#     for distances_list in distances:
#         for dist in distances_list:
#             distances_col.append(dist * r_m)

#     # find neighbors by index and keep those within the chosen radius
#     neighbors = potentials.iloc[indices_col].copy()
#     neighbors['distance'] = distances_col
#     neighbors = neighbors[neighbors['distance'] <= radius]
#     neighbors = neighbors.drop_duplicates()
    
    
#     return neighbors
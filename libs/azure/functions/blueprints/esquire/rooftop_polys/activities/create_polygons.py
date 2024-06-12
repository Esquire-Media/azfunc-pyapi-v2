# File: libs/azure/functions/blueprints/esquire/audiences/daily_audience_generation/activities/create_polygons.py

import os
from azure.durable_functions import Blueprint
import pandas as pd
import os
import googlemaps
from shapely.geometry import Polygon, Point, mapping
import numpy as np
from shapely.ops import orient, transform as shapely_transform
from pyproj import Geod, Proj, transform as pyproj_transform
from functools import partial
from libs.azure.key_vault import KeyVaultClient
import logging

bp: Blueprint = Blueprint()

chunk_size = int(os.environ["chunk_size"])
max_sql_parameters = 1000
# maximum number of parameters that MS SQL can parse is ~2100

# activity to validate the addresses
@bp.activity_trigger(input_name="queries")
def activity_rooftopPolys_createPolygons(queries: dict):
    kv = KeyVaultClient('google-service')

    gmaps = googlemaps.Client(key=kv.get_secret('google-api-key').value)

    # send an API call to GoogleMaps to get approximate rooftop polygons
    gmaps_passed = execute_google_maps_call(gmaps=gmaps, query_list=queries)
    logging.warning(f"{len(gmaps_passed)} passed gmaps")

    # send an API call to GoogleRoads to weed out polygons that are too close to a roadway
    if len(gmaps_passed):
        roads_passed = execute_roads_call(gmaps=gmaps, df=gmaps_passed)
        logging.warning(f"{len(roads_passed)} passed roads")

        # convert shapely polygon type to GeoJSON string
        if len(roads_passed) and 'polygon' in roads_passed.columns:
            roads_passed['geojson'] = roads_passed['polygon'].apply(lambda x: mapping(x))
            return roads_passed[['query','geojson']].to_dict(orient='records')
        else:
            return {}
    else:
        return {}

def execute_google_maps_call(gmaps:googlemaps.Client, query_list:list, min_frame_area:int=25, max_frame_area:int=3000) -> pd.DataFrame:
    """
    Connects to the google maps client and sends each address through as a query. 
    
    # Params:
    gmaps           : GoogleMaps python client.
    query_list      : List of query strings to geocode.
    min_frame_area  : Minimum bound for valid frame area (in square meters).
    max_frame_area  : Maximum bound for valid frame area (in square meters).
    """
    # send googlemaps queries to get bounding box of each rooftop
    gmaps_results_list = []

    for i, query in enumerate(query_list):
        res = gmaps.geocode(query)
        if len(res): # skip if response is an empty list (rare case if couldn't find a match)
            gmaps_results_list.append({
                'index':i,
                'query':query,
                'result':res[0]
            })

    gmaps_results = pd.json_normalize(gmaps_results_list)
    if len(gmaps_results) and 'result.geometry.bounds.northeast.lng' in gmaps_results.columns:
        gmaps_results = gmaps_results.dropna(subset=['result.geometry.bounds.northeast.lng'])

        # convert google location bounds into a shapely Polgyon object
        gmaps_results['polygon'] = gmaps_results.apply(
            lambda row:
            Polygon([
                [row['result.geometry.bounds.northeast.lng'], row['result.geometry.bounds.northeast.lat']],
                [row['result.geometry.bounds.northeast.lng'], row['result.geometry.bounds.southwest.lat']],
                [row['result.geometry.bounds.southwest.lng'], row['result.geometry.bounds.southwest.lat']],
                [row['result.geometry.bounds.southwest.lng'], row['result.geometry.bounds.northeast.lat']],
                [row['result.geometry.bounds.northeast.lng'], row['result.geometry.bounds.northeast.lat']]
            ])
            ,axis=1
        )

        # get area of Polygon in square meters
        geod = Geod(ellps="WGS84")
        gmaps_results['area'] = gmaps_results['polygon'].apply(
            lambda poly: abs(geod.geometry_area_perimeter(orient(poly))[0])
        )

        # drop results where area is outside of the bounds: (25m^2, 3000m^2)
        gmaps_passed = gmaps_results[gmaps_results['area'].between(min_frame_area, max_frame_area)].copy()

        return gmaps_passed
    else:
        return pd.DataFrame()

def execute_roads_call(gmaps:googlemaps.Client, df:pd.DataFrame, road_buffer:int=3) -> pd.DataFrame:
    """
    Connects to the google roads client and checks each rooftop polygon for road proximity. 
    
    # Params:
    gmaps           : GoogleMaps python client.
    df              : DataFrame of GoogleMaps address data.
    road_buffer     : Distance in meters to consider "too close" to a roadway.
    
    # Returns:
    roads_passed : Dataframe of addresses that passed the roads test and were at least the minimum distance from the nearest road.
    roads_failed : Dataframe of failed addresses that were too close to a road.
    """

    # iterate in chunks of 50 rows (maximum passable to the Roads query)
    passed_road_test = []

    for name, g_chunk in df.groupby(np.arange(len(df))//50):
        # print(len(g_chunk))
        g_chunk = g_chunk.reset_index(drop=True)

        # get a list of points to pass (bucket of 50 max)
        points = [[x['result.geometry.location.lat'],x['result.geometry.location.lng']] for i, x in g_chunk.iterrows()]
        res = gmaps.nearest_roads(points=points)

        # check each returned road point for distance to polygon
        roads = pd.json_normalize(res)
        for h_idx, house in g_chunk.iterrows():
            road_overlap = False

            # handle case where no roads are returned
            if len(roads):
                road_candidates = roads[roads['originalIndex']==h_idx]
            else:
                road_candidates = pd.DataFrame()

            for r_idx, road in road_candidates.iterrows():
                road_circle = latlon_circle(lat=road['location.latitude'], lon=road['location.longitude'], radius=road_buffer)
                int_area = house['polygon'].intersection(road_circle).area
                
                # if there is a nonzero intersection between the point+buffer and the house polygon, then it overlaps with a road too closely
                if int_area > 0:
                    road_overlap = True
                    break

            # add to list of passed/failed houses
            if not road_overlap:
                passed_road_test.append(house)

    # collect roads info and drop the temp index columns
    roads_passed = pd.DataFrame(passed_road_test)
    if len(roads_passed):
        roads_passed = roads_passed.drop(columns=['index']).reset_index(drop=True)

    return roads_passed


def latlon_circle(lat:float, lon:float, radius:float):
    """
    Creates a buffer circle around a latlong point with a given radius in meters.

    # Params:
    lat : Latitude in degrees.
    lon : Longitude in degrees.
    Radius : Radius in meters.

    # Returns:
    circle_poly : Shapely polygon representing a circle with a given radius and centerpoint.
    """
    
    local_azimuthal_projection = "+proj=aeqd +R=6371000 +units=m +lat_0={} +lon_0={}".format(
        lat, lon
    )
    wgs84_to_aeqd = partial(
        pyproj_transform,
        Proj("+proj=longlat +datum=WGS84 +no_defs"),
        Proj(local_azimuthal_projection),
    )
    aeqd_to_wgs84 = partial(
        pyproj_transform,
        Proj(local_azimuthal_projection),
        Proj("+proj=longlat +datum=WGS84 +no_defs"),
    )

    point_transformed = shapely_transform(
        wgs84_to_aeqd, 
        Point(float(lon), float(lat))
    )
    buffer = point_transformed.buffer(radius)
    # Get the polygon with lat lon coordinates
    circle_poly = shapely_transform(aeqd_to_wgs84, buffer)
    return circle_poly
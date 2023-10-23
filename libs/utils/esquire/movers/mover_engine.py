import json
import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from haversine import haversine, Unit
from shapely.geometry.polygon import Polygon
from shapely.geometry import box
from libs.utils.geometry import (
    latlon_buffer,
    points_in_poly_numpy,
    points_in_multipoly_numpy,
)
from libs.utils.h3 import hex_intersections, data_to_shape
import pandas as pd

from libs.data import from_bind
from sqlalchemy.orm import Session

# NOTE: Mover data does not yet have lat/long columns, so the polygon/point methods don't currently work.


class MoverEngine:
    def __init__(self, provider):
        self.provider = provider

    def load_from_zipcodes(
        self, start_date: date, end_date: date, zipcodes: list, counts: bool = False
    ):
        """
        Given a list of zipcodes, return all mover addresses registered to those zipcodes.
        Uses string matching, and does not invoke any actual zipcode polygons for geography-based search.

        Params:
        start_date : Pull movers after this date (inclusive).
        end_date   : Pull movers before this date (inclusive).
        zipcodes   : List of 5-digit zipcodes as strings or ints.
        counts     : If true, will only return mover counts instead of the address list. Results in significantly faster queries.

        Returns:
        results  : Pandas DataFrame of mover addresses within the given zipcode list.
        """

        # connect to Synapse and pull mover data between selected dates
        session: Session = self.provider.connect()
        movers = self.provider.models["dbo"]["movers"]

        if counts:
            # return count of selected rows
            results = (
                session.query(
                    movers.address,
                    movers.city,
                    movers.state,
                    movers.zipcode,
                    movers.plus4Code,
                )
                .filter(
                    movers.date >= start_date,
                    movers.date <= end_date,
                    movers.zipcode.in_(zipcodes),
                )
                .count()
            )
        else:
            # return all selected rows
            results = pd.DataFrame(
                session.query(
                    movers.address,
                    movers.city,
                    movers.state,
                    movers.zipcode,
                    movers.plus4Code,
                )
                .filter(
                    movers.date >= start_date,
                    movers.date <= end_date,
                    movers.zipcode.in_(zipcodes),
                )
                .all()
            )

        return results

    # def load_from_polygon_bbox(self, start_date:date, end_date:date, polygon:Polygon, counts:bool=False):
    #     """
    #     Given a query polygon, return all mover addresses within that polygon.
    #     Uses a latlong bounding box to pull candidates, then runs point-in-polygon checks against the query polygon.

    #     Params:
    #     start_date : Pull movers after this date (inclusive).
    #     end_date   : Pull movers before this date (inclusive).
    #     polygon    : Polygon data as a shapely.geometry.polygon.Polygon .
    #     counts     : If true, will only return mover counts instead of the address list. Results in significantly faster queries.

    #     Returns:
    #     results  : Pandas DataFrame of mover addresses within the given polygon.
    #     """
    #     # rectangle polygon buffer to use as an initial pull
    #     # we use this because we can filter on the latlong field easier with a rectangle
    #     bounds = box(*polygon.bounds)

    #     # pull address data from the partially-intersecting hexes
    #     with app.app_context():
    #         cursor = db.engines.get("audiences").connect()
    #         if counts:
    #             fields = f"COUNT(*) as [count]"
    #         else:
    #             fields = "[address], [city], [state], [zipcode], [plus4Code], [latitude], [longitude]"
    #         bounds_query = f"""
    #         SELECT
    #             {fields}
    #         FROM [dbo].[movers]
    #         WHERE (
    #             [date] >= '{start_date}'
    #             AND [date] <= '{end_date}'
    #             AND [latitude] >= {min(bounds.exterior.coords.xy[1])}
    #             AND [latitude] <= {max(bounds.exterior.coords.xy[1])}
    #             AND [longitude] >= {min(bounds.exterior.coords.xy[0])}
    #             AND [longitude] <= {max(bounds.exterior.coords.xy[0])}
    #         )
    #         """
    #         bounds_data = pd.read_sql(bounds_query, cursor)

    #     # do point in polygon checks for the partially-intersecting hexes
    #     bounds_data.loc[:, 'in_polygon'] = points_in_poly_numpy(
    #         x=bounds_data['longitude'].values,
    #         y=bounds_data['latitude'].values,
    #         poly=polygon.exterior.coords
    #     )

    #     # concat the validated partial-intersection data with the full-intersection data
    #     results = bounds_data[bounds_data['in_polygon']].drop(columns=['in_polygon'])

    #     return results

    # def load_from_polygon(self, start_date:date, end_date:date, polygon_wkt:Polygon, counts:bool=False, resolution:int=5):
    #     """
    #     Given a query polygon, return all mover addresses within that polygon.
    #     Uses h3 indexing to pull candidates, then runs point-in-polygon checks against the query polygon.

    #     Params:
    #     start_date : Pull movers after this date (inclusive).
    #     end_date   : Pull movers before this date (inclusive).
    #     polygon_wkt    : Polygon data as a shapely.geometry.polygon.Polygon .
    #     counts     : If true, will only return mover counts instead of the address list. Results in significantly faster queries.

    #     Returns:
    #     results  : Pandas DataFrame of mover addresses within the given polygon.
    #     """

    #     # convert WKT to shapely object
    #     polygon = data_to_shape(polygon_wkt)

    #     # find H3 hexes which intersect with the query area
    #     hexes = hex_intersections(polygon, resolution=resolution)

    #     # generate a list of indexes by their overlap type with the query polygon
    #     partial_indexes = hexes[hexes['intersection']=='partial']['id'].unique()
    #     full_indexes = hexes[hexes['intersection']=='full']['id'].unique()

    #     # toggle COUNTS or SELECT statement
    #     if counts:
    #         fields = f"COUNT(*) as [count]"
    #     else:
    #         fields = "[address], [city], [state], [zipcode], [plus4Code], [latitude], [longitude]"

    #     # pull mover data from the fully-intersecting hexes
    #     if len(full_indexes):
    #         with app.app_context():
    #             cursor = db.engines.get("audiences").connect()

    #             # build query
    #             full_indexes_str = ','.join([f"'{index}'" for index in full_indexes])
    #             full_index_query = f"""
    #             SELECT
    #                 {fields}
    #             FROM [dbo].[movers]
    #             WHERE [h3_index] IN ({full_indexes_str})
    #             AND [date] >= '{start_date}'
    #             AND [date] <= '{end_date}'
    #             """
    #             full_index_data = pd.read_sql(full_index_query, cursor)
    #     else:
    #         full_index_data = pd.DataFrame()

    #     # pull mover data from the partially-intersecting hexes
    #     with app.app_context():
    #         cursor = db.engines.get("audiences").connect()
    #         partial_indexes_str = ','.join([f"'{index}'" for index in partial_indexes])
    #         partial_index_query = f"""
    #         SELECT
    #             [address], [city], [state], [zipcode], [plus4Code], [latitude], [longitude]
    #         FROM [dbo].[movers]
    #         WHERE [h3_index] IN ({partial_indexes_str})
    #         AND [date] >= '{start_date}'
    #         AND [date] <= '{end_date}'
    #         """
    #         partial_index_data = pd.read_sql(partial_index_query, cursor)

    #     # do point in polygon checks for the partially-intersecting hexes
    #     if polygon.geom_type == 'Polygon':
    #         partial_index_data.loc[:, 'in_polygon'] = points_in_poly_numpy(
    #             partial_index_data['longitude'].values,
    #             partial_index_data['latitude'].values,
    #             np.array(polygon.exterior.coords)
    #         )
    #     elif polygon.geom_type == 'MultiPolygon':
    #         partial_index_data.loc[:, 'in_polygon'] = points_in_multipoly_numpy(
    #             partial_index_data['longitude'].values,
    #             partial_index_data['latitude'].values,
    #             polygon
    #         )
    #     else:
    #         raise Exception(f"geom_type '{polygon.geom_type}' is not supported")

    #     if counts == True:
    #         # sum the counts of full and partial data, where they exist
    #         if 'count' in full_index_data.columns:
    #             results = pd.DataFrame([{"count": full_index_data['count'].iloc[0] + len(partial_index_data[partial_index_data['in_polygon']])}])
    #         else:
    #             results = pd.DataFrame([{"count": len(partial_index_data[partial_index_data['in_polygon']])}])
    #     else:
    #         # concat the validated partial-intersection data with the full-intersection data
    #         results = pd.concat([
    #             partial_index_data[partial_index_data['in_polygon']].drop(columns=['in_polygon']),
    #             full_index_data
    #         ])

    #     return results

    # def load_from_point(self, start_date:date, end_date:date, latitude:float, longitude:float, radius:int=1000, counts:bool=False):
    #     """
    #     Given a latlong point and radius in meters, return all mover addresses within that area.
    #     Uses a latlong bounding box to pull candidates, then runs point-in-polygon checks against the query polygon.

    #     Params:
    #     start_date  : Pull movers after this date (inclusive).
    #     end_date    : Pull movers before this date (inclusive).
    #     lat         : Latitude of query centerpoint.
    #     long        : Longitude of query centerpoint.
    #     radius      : Radius of query area, in meters.
    #     counts      : If true, will only return mover counts instead of the address list. Results in significantly faster queries.

    #     Returns:
    #     results     : Pandas DataFrame of mover addresses within the given query area.
    #     """
    #     # build a circle to represent the point-radius search area
    #     polygon = latlon_buffer(lat=latitude, lon=longitude, radius=radius, cap_style=1)

    #     # load results as polygon using the point-radius circle
    #     results = self.load_from_polygon(polygon=polygon, start_date=start_date, end_date=end_date, counts=counts)

    #     # calculate distance from centerpoint (for point/polygon queries only)
    #     results['distance_miles'] = results.apply(
    #         lambda x:
    #         round(haversine(
    #             [x['latitude'],x['longitude']],
    #             [latitude,longitude],
    #             unit=Unit.MILES
    #         ),3),
    #         axis=1
    #     )

    #     return results

import pandas as pd
import numpy as np
from datetime import date
from haversine import haversine, Unit
from shapely.geometry.polygon import Polygon
from libs.utils.geometry import (
    latlon_buffer,
    points_in_poly_numpy,
    points_in_multipoly_numpy,
)
from libs.utils.h3 import hex_intersections, data_to_shape
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import func

class MoverEngine:
    def __init__(self, provider):
        self.provider = provider

    def load_from_zipcodes(self, start_date:date, end_date:date, zipcodes:list, counts:bool=False):
        """
        Given a list of zipcodes, return all mover addresses registered to those zipcodes.
        Uses string matching, and does not invoke any actual zipcode polygons for geography-based search.

        Params:
        start_date  : Pull movers on or after this date.
        end_date    : Pull movers on or before this date.
        zipcodes    : List of 5-digit zipcodes as strings or ints.
        counts      : If true, will only return mover counts instead of the address list. Results in significantly faster queries.

        Returns:
        results     : Pandas DataFrame of mover addresses within the given zipcode list.
        """

        # connect to Synapse and pull mover data between selected dates
        session: Session = self.provider.connect()
        movers = self.provider.models["dbo"]["movers"]

        if counts:
            # return count of movers in selected zipcodes, grouped by zipcode
            results = (
                session.query(
                    movers.zipcode,
                    func.count(movers.address),
                )
                .filter(
                    movers.date >= start_date,
                    movers.date <= end_date,
                    movers.zipcode.in_(zipcodes),
                )
                .group_by(
                    movers.zipcode
                ).all()
            )
        else:
            # return the address data for all movers in selected zipcodes
            results = pd.DataFrame(
                session.query(
                    movers.address,
                    movers.city,
                    movers.state,
                    movers.zipcode,
                    movers.plus4Code,
                    movers.latitude,
                    movers.longitude
                )
                .filter(
                    movers.date >= start_date,
                    movers.date <= end_date,
                    movers.zipcode.in_(zipcodes),
                )
                .all()
            )

        return results


    def load_from_polygon(self, start_date:date, end_date:date, polygon:Polygon, counts:bool=False, resolution:int=5):
        """
        Given a query polygon, return all mover addresses within that polygon.
        Uses h3 indexing to pull candidates, then runs point-in-polygon checks against the query polygon.

        Params:
        start_date      : Pull movers on or after this date.
        end_date        : Pull movers on or before this date.
        polygon         : Polygon data as a shapely.geometry.polygon.Polygon
        counts          : If true, will only return mover counts instead of the address list. Results in significantly faster queries.

        Returns:
        results         : Pandas DataFrame of mover addresses within the given polygon.
        """

        # connect to Synapse and pull mover data between selected dates
        session: Session = self.provider.connect()
        movers = self.provider.models["dbo"]["movers"]

        # convert WKT to shapely object
        polygon = data_to_shape(polygon)

        # find H3 hexes which intersect with the query area
        hexes = hex_intersections(polygon, resolution=resolution)

        # generate a list of indexes by their overlap type with the query polygon
        partial_indexes = hexes[hexes['intersection']=='partial']['id'].unique()
        full_indexes = hexes[hexes['intersection']=='full']['id'].unique()

        # if a COUNTS query, get count of full-enveloped indexes (partial indexes will need PIP checks)
        if len(full_indexes):
            if counts:
                # count of movers in fully-enveloped h3 hexes
                full_index_data = (
                    session.query(
                        movers.address
                    )
                    .filter(
                        movers.date >= start_date,
                        movers.date <= end_date,
                        movers.h3_index.in_(full_indexes)
                    )
                    .count()
                )
            else:
                # selection of movers in fully-enveloped h3 hexes
                full_index_data = pd.DataFrame(
                    session.query(
                        movers.address,
                        movers.city,
                        movers.state,
                        movers.zipcode,
                        movers.plus4Code,
                        movers.latitude,
                        movers.longitude
                    )
                    .filter(
                        movers.date >= start_date,
                        movers.date <= end_date,
                        movers.h3_index.in_(full_indexes),
                    )
                    .all()
                )
        else:
            # if no full indexes to pre-query, instantiate empty results
            if counts:
                full_index_data = 0
            else:
                full_index_data = pd.DataFrame()

        # query for movers in hexes that partially-intersect the query polygon
        if len(partial_indexes):
            if counts:
                partial_index_data = (
                    session.query(
                        movers.address,
                    )
                    .filter(
                        movers.date >= start_date,
                        movers.date <= end_date,
                        movers.h3_index.in_(partial_indexes)
                    )
                    .count()
                )
            else:
                partial_index_data = pd.DataFrame(
                    session.query(
                        movers.address,
                        movers.city,
                        movers.state,
                        movers.zipcode,
                        movers.plus4Code,
                        movers.latitude,
                        movers.longitude
                    )
                    .filter(
                        movers.date >= start_date,
                        movers.date <= end_date,
                        movers.h3_index.in_(partial_indexes),
                    )
                    .all()
                )

            # conduct point-in-polygon checks to determine which movers from the partial hexes actually fall inside the query polygon
            if polygon.geom_type == 'Polygon':
                partial_index_data.loc[:, 'in_polygon'] = points_in_poly_numpy(
                    partial_index_data['longitude'].values, 
                    partial_index_data['latitude'].values, 
                    np.array(polygon.exterior.coords)
                )
            elif polygon.geom_type == 'MultiPolygon':
                partial_index_data.loc[:, 'in_polygon'] = points_in_multipoly_numpy(
                    partial_index_data['longitude'].values, 
                    partial_index_data['latitude'].values, 
                    polygon
                )
            else:
                raise Exception(f"geom_type '{polygon.geom_type}' is not supported for point-in-polygon checks.")
        else:
            if counts:
                partial_index_data = 0
            else:
                partial_index_data = pd.DataFrame()


        # combine the full-hex and partial-hex data into one output
        if counts == True:
            if 'count' in full_index_data.columns:
                results = pd.DataFrame([{"count": full_index_data['count'].iloc[0] + len(partial_index_data[partial_index_data['in_polygon']])}])
            else:
                results = pd.DataFrame([{"count": len(partial_index_data[partial_index_data['in_polygon']])}])
        else:
            # concat the validated partial-intersection data with the full-intersection data
            results = pd.concat([
                partial_index_data[partial_index_data['in_polygon']].drop(columns=['in_polygon']),
                full_index_data
            ])

        return results


    def load_from_point(self, start_date:date, end_date:date, latitude:float, longitude:float, radius:int=1000, counts:bool=False):
        """
        Given a latlong point and radius in meters, return all mover addresses within that area.
        Uses a latlong bounding box to pull candidates, then runs point-in-polygon checks against the query polygon.

        Params:
        start_date  : Pull movers on or after thi.
        end_date    : Pull movers on or before thi.
        lat         : Latitude of query centerpoint.
        long        : Longitude of query centerpoint.
        radius      : Radius of query area, in meters.
        counts      : If true, will only return mover counts instead of the address list. Results in significantly faster queries.

        Returns:
        results     : Pandas DataFrame of mover addresses within the given query area.
        """
        # build a circle to represent the point-radius search area
        polygon = latlon_buffer(lat=latitude, lon=longitude, radius=radius, cap_style=1)

        # load results as polygon using the point-radius circle
        results = self.load_from_polygon(polygon=polygon, start_date=start_date, end_date=end_date, counts=counts)

        # calculate distance from centerpoint (for point/polygon queries only)
        results['distance_miles'] = results.apply(
            lambda x:
            round(haversine(
                [x['latitude'],x['longitude']],
                [latitude,longitude],
                unit=Unit.MILES
            ),3),
            axis=1
        )

        return results

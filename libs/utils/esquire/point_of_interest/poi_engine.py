import pandas as pd
import numpy as np
from datetime import date
from haversine import haversine, Unit
from shapely.geometry.polygon import Polygon
import json
from shapely.wkt import loads as wkt_loads
from libs.utils.geometry import (
    latlon_buffer,
    points_in_poly_numpy,
    points_in_multipoly_numpy,
)
from libs.utils.h3 import hex_intersections
import pandas as pd
from sqlalchemy.orm import Session
from libs.utils.python import literal_eval_list


class POIEngine:
    def __init__(self, provider_poi, provider_esq):
        self.provider_poi = provider_poi
        self.provider_esq = provider_esq

    def load_from_polygon(
        self,
        polygon_wkt: str,
        categories: list = None
    ):
        """
        Given a query polygon, return all POI data within that polygon.
        Uses an indexed search method to pull data in hexes that fully or partially intersect the query polygon.

        Params:
        polgon_wkt  : Polygon data as a WKT str.
        categories  : A list of integers representing FSQ category IDs.

        Returns:
        results     : Pandas DataFrame of POI data within the given polygon.
        """
        # connect to the Synapse tables necessary to pull POI data and cross-reference with ESQ locations
        session_poi: Session = self.provider_poi.connect()
        session_esq: Session = self.provider_esq.connect()
        poi = self.provider_poi.models["dbo"]["poi"]
        locations = self.provider_esq.models["esquire"]["locations"]
        geoframes = self.provider_esq.models["esquire"]["geoframes"]

        # find H3 hexes which intersect with the query area
        hexes = hex_intersections(polygon_wkt, resolution=5)

        # generate a list of indexes by their overlap type with the query polygon
        partial_indexes = hexes[hexes["intersection"] == "partial"]["id"].unique()
        full_indexes = hexes[hexes["intersection"] == "full"]["id"].unique()

        # pull POI data from the fully-intersecting hexes
        if len(full_indexes):
            full_index_data = pd.DataFrame(
                session_poi.query(
                    poi.fsq_id,
                    poi.name,
                    poi.latitude,
                    poi.longitude,
                    poi.address,
                    poi.address_extended,
                    poi.city,
                    poi.dma,
                    poi.state,
                    poi.zipcode,
                    poi.country,
                    poi.neighborhood,
                    poi.po_box,
                    poi.date_created,
                    poi.date_refreshed,
                    poi.fsq_category_ids,
                    poi.fsq_category_labels,
                    poi.fsq_chain_id,
                    poi.fsq_chain_name,
                    poi.parent_id,
                    poi.census_block_id,
                    poi.popularity,
                    poi.venue_reality_bucket,
                    poi.provenance_rating,
                    poi.date_closed,
                    poi.closed_bucket,
                    poi.h3_index,
                )
                .filter(
                    poi.h3_index.in_(full_indexes),
                )
                .all()
            )
        else:
            full_index_data = pd.DataFrame()

        if len(partial_indexes):
            partial_index_data = pd.DataFrame(
                session_poi.query(
                    poi.fsq_id,
                    poi.name,
                    poi.latitude,
                    poi.longitude,
                    poi.address,
                    poi.address_extended,
                    poi.city,
                    poi.dma,
                    poi.state,
                    poi.zipcode,
                    poi.country,
                    poi.neighborhood,
                    poi.po_box,
                    poi.date_created,
                    poi.date_refreshed,
                    poi.fsq_category_ids,
                    poi.fsq_category_labels,
                    poi.fsq_chain_id,
                    poi.fsq_chain_name,
                    poi.parent_id,
                    poi.census_block_id,
                    poi.popularity,
                    poi.venue_reality_bucket,
                    poi.provenance_rating,
                    poi.date_closed,
                    poi.closed_bucket,
                    poi.h3_index,
                ).filter(
                    poi.h3_index.in_(partial_indexes),
                )
                .all()
            )
        else:
            partial_index_data = pd.DataFrame()

        # do point in polygon checks for the partially-intersecting hexes
        if wkt_loads(polygon_wkt).geom_type == 'Polygon':
            partial_index_data.loc[:, 'in_polygon'] = points_in_poly_numpy(
                partial_index_data['longitude'].values, 
                partial_index_data['latitude'].values, 
                np.array(wkt_loads(polygon_wkt).exterior.coords)
            )
        elif wkt_loads(polygon_wkt).geom_type == 'MultiPolygon':
            partial_index_data.loc[:, 'in_polygon'] = points_in_multipoly_numpy(
                partial_index_data['longitude'].values, 
                partial_index_data['latitude'].values, 
                wkt_loads(polygon_wkt)
            )
        else:
            raise Exception(f"geom_type '{wkt_loads(polygon_wkt).geom_type}' is not supported")
        
        # concat the validated partial-intersection data with the full-intersection data
        results = pd.concat([
            partial_index_data[partial_index_data['in_polygon']].drop(columns=['in_polygon']),
            full_index_data
        ])

        # format results
        results['fsq_category_ids'] = results['fsq_category_ids'].apply(lambda x: literal_eval_list(x))
        results['fsq_category_labels'] = results['fsq_category_labels'].apply(lambda x: literal_eval_list(x))

        # filter categories if passed
        if categories != None:
            tax = TaxonomyEngine()
            # create a list of categories and any children they have
            category_children = []
            for category in categories:
                [category_children.append(int(cat)) for cat in tax.get_category_children(str(category))]
            # dedupe categories
            category_children = list(set(category_children))

            # filter results by category
            results = results[results['fsq_category_ids'].apply(lambda list: any([cat in list for cat in category_children]))]

        # check for existing ESQ locations among the FSQ results
        if len(results):
            esq_exists = pd.DataFrame(
                session_esq.query(
                    geoframes.esq_id,
                    locations.foursquare.label('fsq_id')            
                )
                .filter(
                    locations.id == geoframes.location_id,
                    locations.foursquare.in_(
                        results['fsq_id']
                    )
                )
                .all()
            )
            # merge existing locations with the new pull of FSQ competitors
            results = pd.merge(
                results,
                esq_exists,
                on='fsq_id',
                how='outer'
            )
            results['esq_id'] = results['esq_id'].fillna('null')

        return results

    def load_from_point(
        self,
        latitude: float,
        longitude: float,
        radius: float,
        categories: list = None,
    ):
        """
        Given a latlong point and radius in meters, return all POI data within that area.
        Uses an indexed search method to pull data in hexes that fully or partially intersect the query area.

        Params:
        lat         : Latitude of query centerpoint.
        long        : Longitude of query centerpoint.
        radius      : Radius of query area, in meters.
        categories  : A list of integers representing FSQ category IDs.

        Returns:
        results     : Pandas DataFrame of POI data within the given query area.
        """

        # build a circle to represent the point-radius search area
        circle = latlon_buffer(lat=latitude, lon=longitude, radius=radius, cap_style=1)

        # load results as polygon using the point-radius circle
        results = self.load_from_polygon(
            polygon_wkt=circle.wkt, categories=categories
        )

        # calculate distance from centerpoint (for point/polygon queries only)
        results["distance_miles"] = results.apply(
            lambda x: round(
                haversine(
                    [x["latitude"], x["longitude"]],
                    [latitude, longitude],
                    unit=Unit.MILES,
                ),
                3,
            ),
            axis=1,
        )
        return results


class TaxonomyEngine:
    """
    Engine for parsing FSQ category IDs and unraveling the nested relationships within the category taxonomy.
    """
    def __init__(self):

        # load taxonomy json from file
        with open('libs/utils/esquire/point_of_interest/integrated_category_taxonomy.json', encoding='utf-8') as infile:
            taxonomy_js = json.load(infile)

        # convert to Pandas dataframe and apply formatting
        self.taxonomy = pd.DataFrame(taxonomy_js).T.reset_index()
        self.taxonomy['name'] = self.taxonomy['full_label'].apply(lambda x: x[-1])
        self.taxonomy['parent'] = self.taxonomy['parents'].apply(lambda x: x[0] if len(x) else None)
        self.taxonomy = self.taxonomy.drop(columns=['parents','full_label'])

    def get_category_children(self, category_id:str) -> list:
        """
        Given a category id, return that id and all of its children ids using recursion.

        Params:
        category_id     : any valid fsq_category_id.

        Returns:
        A list of category_ids which are children of the originally-passed id, including the original id.
        """

        # search children of the current category
        children_df = self.taxonomy[self.taxonomy['parent']==category_id]

        # recursively iterate and store the child rows
        children_list = []
        if len(children_df):
            for idx in children_df['index']:
                info = self.get_category_children(category_id=idx)
                [children_list.append(d) for d in info]

        # return list of values
        return [
            category_id,    # the current category
            *children_list  # all children of the current category
        ]
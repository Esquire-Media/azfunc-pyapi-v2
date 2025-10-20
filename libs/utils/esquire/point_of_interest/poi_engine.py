from haversine import haversine, Unit
from libs.utils.geometry import latlon_buffer
from libs.utils.h3 import hex_intersections
from shapely.ops import unary_union
from shapely.wkt import loads as wkt_loads
from sklearn.neighbors import BallTree
from sqlalchemy.orm import Session
import orjson as json, pandas as pd, numpy as np


class POIEngine:
    def __init__(self, provider):
        self.provider = provider

    def load_from_polygon(self, polygon_wkt: str, categories: list = None):
        """
        Given a query polygon, return all POI data within that polygon.
        Uses an indexed search method to pull data in hexes that fully or partially intersect the query polygon.

        Params:
        polygon_wkt  : Polygon data as a WKT str.
        categories   : A list of integers representing FSQ category IDs.

        Returns:
        poi_data     : Pandas DataFrame of POI data (with associated category lists) within the given polygon.
        """
        # connect to the Synapse tables necessary to pull POI data and cross-reference with ESQ locations
        session: Session = self.provider.connect()

        # find H3 hexes which intersect with the query area
        hexes = hex_intersections(polygon_wkt, resolution=5)
        h3_indexes = list(
            set(
                list(hexes[hexes["intersection"] == "full"]["id"].unique())
                + list(hexes[hexes["intersection"] == "partial"]["id"].unique())
            )
        )

        # Build the base query that applies the H3 and spatial filters
        h3_ids = "','".join(h3_indexes)
        query = f"""
            SELECT
                fsq.*,
	            COALESCE(chain.chain_name, fsq.name) AS chain_name
            FROM poi.foursquare AS fsq
            LEFT JOIN poi.foursquare_poi_chains AS cha
                ON fsq.id = cha.poi_id
            LEFT JOIN poi.foursquare_chains AS chain
                ON cha.chain_id = chain.chain_id
            WHERE 
                h3_index IN ('{h3_ids}')
                AND public.ST_Within(
                    fsq.point,
                    public.ST_SetSRID(public.ST_GeomFromText('{polygon_wkt}'), 4326)
                )
        """
        if categories is not None:
            # Assuming 'categories' is a list of string ids, join them for the SQL IN clause.
            cat_ids = ",".join([str(id) for id in categories])
            query = f"""
                WITH RECURSIVE cat_tree AS (
                    SELECT id
                    FROM poi.foursquare_category
                    WHERE id IN ({cat_ids})
                    UNION ALL
                    SELECT child.id
                    FROM poi.foursquare_category child
                    JOIN cat_tree d 
                        ON child.parent_id = d.id
                )
                SELECT 
                    fsq.*,
	                COALESCE(chain.chain_name, fsq.name) AS chain_name
                FROM poi.foursquare AS fsq
                JOIN poi.foursquare_poi_categories AS cat
                    ON fsq.id = cat.poi_id
                LEFT JOIN poi.foursquare_poi_chains AS cha
                    ON fsq.id = cha.poi_id
                LEFT JOIN poi.foursquare_chains AS chain
                    ON cha.chain_id = chain.chain_id
                WHERE 
                    h3_index IN ('{h3_ids}')
                    AND cat.category_id IN (SELECT id FROM cat_tree)
                    AND public.ST_Within(
                        fsq.point::geometry(point),
                        public.ST_SetSRID(public.ST_GeomFromText('{polygon_wkt}'), 4326)
                    )
            """

        results = pd.read_sql(query, session.connection()).rename(columns={"id": "fsq_id"})

        if results.empty:
            return results

        # check for existing ESQ locations among the FSQ results
        if len(results):
            esq_exists = pd.read_sql(
                f"""
                    SELECT
                        id,
                        public.ST_AsText(public.ST_Centroid(public.ST_Collect(public.ST_GeomFromGeoJSON(feature->'geometry')))) AS centroid_wkt
                    FROM 
                        keystone."TargetingGeoFrame",
                        jsonb_array_elements(polygon->'features') AS feature
                    WHERE 
                        source = '' AND
		                public.ST_Within(
                            public.ST_Centroid(public.ST_GeomFromGeoJSON(feature->'geometry')), 
                            public.ST_SetSRID(public.ST_GeomFromText('{polygon_wkt}'), 4326)
                        )
                    GROUP BY
                        id
                """,
                session.connection(),
            )

            if len(esq_exists):
                # Parse centroid_wkt to get latitude and longitude
                esq_exists["geometry"] = esq_exists["centroid_wkt"].apply(wkt_loads)
                esq_exists["esq_longitude"] = esq_exists["geometry"].apply(
                    lambda geom: geom.x
                )
                esq_exists["esq_latitude"] = esq_exists["geometry"].apply(
                    lambda geom: geom.y
                )
                esq_exists.drop(columns=["geometry", "centroid_wkt"], inplace=True)

                # Convert lat/lon to radians
                results["lat_rad"] = np.deg2rad(results["latitude"])
                results["lon_rad"] = np.deg2rad(results["longitude"])
                esq_exists["esq_lat_rad"] = np.deg2rad(esq_exists["esq_latitude"])
                esq_exists["esq_lon_rad"] = np.deg2rad(esq_exists["esq_longitude"])

                # Build BallTree with ESQ centroids
                esq_tree = BallTree(
                    np.vstack((esq_exists["esq_lat_rad"], esq_exists["esq_lon_rad"])).T,
                    metric="haversine",
                )

                # Query nearest neighbor for each result
                distances, indices = esq_tree.query(
                    np.vstack((results["lat_rad"], results["lon_rad"])).T, k=1
                )

                # Convert distance from radians to meters
                earth_radius = 6371000  # meters
                distances_meters = distances.flatten() * earth_radius

                # Get the closest ESQ IDs
                closest_esq_ids = esq_exists.iloc[indices.flatten()]["id"].values

                # Add the closest ESQ IDs meters and distances to results
                results["esq_id"] = closest_esq_ids
                results["distance_to_esq"] = distances_meters

                # Assign 'null' to 'esq_id' if distance is greater than 15 meters
                results.loc[results["distance_to_esq"] > 15, "esq_id"] = None

                # Clean up temporary columns
                results.drop(columns=["lat_rad", "lon_rad"], inplace=True)
                esq_exists.drop(columns=["esq_lat_rad", "esq_lon_rad"], inplace=True)

            # enforce esq_id column and populate null values
            if not "esq_id" in results.columns:
                results["esq_id"] = None

        results = results.dropna(subset=["fsq_id"])
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
        Includes distances in the returned dataset.

        Params:
        lat         : Latitude of query centerpoint.
        long        : Longitude of query centerpoint.
        radius      : Radius of query area, in meters.
        categories  : A list of integers representing FSQ category IDs.

        Returns:
        results     : Pandas DataFrame of POI data within the given query area.
        """

        # build a circle to represent the point-radius search area
        circle = latlon_buffer(
            latitude=latitude, longitude=longitude, radius=radius, cap_style=1
        )

        # load results as polygon using the point-radius circle
        results = self.load_from_polygon(polygon_wkt=circle.wkt, categories=categories)

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

    def load_from_points(
        self,
        points: list[float, float],
        radius: list[float],
        categories: list = None,
    ):
        """
        Given a list of (lat,long) points and a radius in meters, return all POI data within one radius of any of those points.
        Uses an indexed search method to pull data in hexes that fully or partially intersect the query area, and doesn't double-query areas of overlap.
        Does not include distances in the returned dataset.

        Params:
        points      : List of query centerpoints in format (lat, long).
        radius      : Radius for each query area, in meters.
        categories  : A list of integers representing FSQ category IDs.

        Returns:
        results     : Pandas DataFrame of POI data within the given query area.
        """

        # find the unary union of each search area to avoid double-searching areas of overlap
        circle_list = [
            latlon_buffer(
                latitude=point[0], longitude=point[1], radius=radius, cap_style=1
            )
            for point in points
        ]
        polygon_wkt = unary_union(circle_list).wkt

        # query as a polygon using the unary union of each point/radius area
        return self.load_from_polygon(polygon_wkt=polygon_wkt, categories=categories)


class TaxonomyEngine:
    """
    Engine for parsing FSQ category IDs and unraveling the nested relationships within the category taxonomy.
    """

    def __init__(self):

        # load taxonomy json from file
        with open(
            "libs/utils/esquire/point_of_interest/integrated_category_taxonomy.json",
            encoding="utf-8",
        ) as infile:
            taxonomy_js = json.loads(infile.read())

        # convert to Pandas dataframe and apply formatting
        self.taxonomy = pd.DataFrame(taxonomy_js).T.reset_index()
        self.taxonomy["name"] = self.taxonomy["full_label"].apply(lambda x: x[-1])
        self.taxonomy["parent"] = self.taxonomy["parents"].apply(
            lambda x: x[0] if len(x) else None
        )
        self.taxonomy = self.taxonomy.drop(columns=["parents", "full_label"])

    def get_category_children(self, category_id: str) -> list:
        """
        Given a category id, return that id and all of its children ids using recursion.

        Params:
        category_id     : any valid fsq_category_id.

        Returns:
        A list of category_ids which are children of the originally-passed id, including the original id.
        """

        # search children of the current category
        children_df = self.taxonomy[self.taxonomy["parent"] == category_id]

        # recursively iterate and store the child rows
        children_list = []
        if len(children_df):
            for idx in children_df["index"]:
                info = self.get_category_children(category_id=idx)
                [children_list.append(d) for d in info]

        # return list of values
        return [
            category_id,  # the current category
            *children_list,  # all children of the current category
        ]


def recreate_POI_form(sources, query_pool, radius=10):
    """
    Given a set of source addresses and a pool of targets, find the distance between each source/target pair, provided the pair is within one radius distance.
    """
    # conversion factor to miles
    r_m = 3958.8

    # setup a BallTree and query for a max of X miles
    tree = BallTree(
        np.deg2rad(query_pool[["latitude", "longitude"]].values), metric="haversine"
    )
    indices, distances = tree.query_radius(
        np.deg2rad(sources[["latitude", "longitude"]].values),
        r=radius / r_m,
        return_distance=True,
    )

    # a little bit arcane, but it takes the indices and distances, brings them together, and tacks on info for which source they came from
    res = pd.concat(
        [
            pd.concat(
                [pd.DataFrame(index_list), pd.DataFrame(distances[ii]) * r_m], axis=1
            ).assign(close_to=sources.iloc[ii]["address"])
            for ii, index_list in enumerate(indices)
        ]
    ).reset_index(drop=True)
    res.columns = ["POI_index", "distance_miles", "source"]

    # use the indices to merge back onto the main POI data
    final = query_pool.merge(res, how="right", left_index=True, right_on="POI_index")

    return final.sort_values("distance_miles", ascending=True)

import json
import h3
import pandas as pd
from shapely.geometry import shape, box
import shapely.wkt
import geojson

# main entry point is the hex_intersections function

def hex_to_feature(hex_id:str) -> dict:
    """
    Converts a given H3 hex_id to GeoJSON.
    """
    hex_geo = h3.h3_to_geo_boundary(hex_id)
    hex_as_feature = {
        'type':'Feature',
        'id':hex_id,
        'geometry':{
            'type':'Polygon',
            'coordinates':[[[point[1], point[0]] for point in hex_geo]]
        }
    }
    return hex_as_feature

def shape_to_feature(shape:shapely.geometry.shape, id:str=None) -> geojson.Feature:
    """
    Converts a shapely Shape to GeoJSON. Polygons and MultiPolygons supported.
    """
    return geojson.Feature(
        type='Feature',
        geometry=shape, 
        id=id,
    )

def data_to_shape(data) -> shapely.geometry.shape:
    """
    Accepts GJ_dict, GJ_string, or WKT_string data and returns a shapely Shape object.
    """
    # load data into a shapely Shape object
    try:
        # load from geojson dictionary
        poly_as_shape = shape(data['geometry'])
    except:
        try:
            # load from geojson string
            poly = json.loads(data)
            poly_as_shape = shape(poly['geometry'])
        except:
            try:
                # load from wkt string
                poly_as_shape = shapely.wkt.loads(data)
            except:
                try:
                    # load from coordinate string in a double list
                    poly_as_shape = shape({"coordinates":data,"type":"Polygon"})
                except:
                    try:
                        # load from coordinate string in a single list
                        poly_as_shape = shape({"coordinates":[data],"type":"Polygon"})
                    except:
                        raise Exception("Could not convert data to a shapely object. Accepted types are GeoJSON dictionary, GeoJSON string, and WKT string.")

    return poly_as_shape


def hex_intersections(data, resolution:int=8) -> pd.DataFrame:
    """"
    Given a geometry and resolution, returns the H3 hexes that are needed to completely cover the given geometry.

    # Parameters

    * data: The geometry data. Supported types are GeoJSON dictionary, GeoJSON string, and WKT string.

    * resolution: The resolution level at which to generate H3 hexes.

    """
    # get a shapely Shape from the input data (supported types in docstring)
    poly_as_shape = data_to_shape(data)

    # use built-in h3 polyfill to get hexes with centroids inside the bounding box
    if poly_as_shape.geom_type == 'Polygon':
        filled_hexes = h3.polyfill_geojson(geojson=shape_to_feature(poly_as_shape)['geometry'], res=resolution)
    elif poly_as_shape.geom_type == 'MultiPolygon':
        # for multipolygons, use a bounding box that encapsulates the entire bounded area
        bounding_box = box(
            minx=poly_as_shape.bounds[0],
            miny=poly_as_shape.bounds[1],
            maxx=poly_as_shape.bounds[2],
            maxy=poly_as_shape.bounds[3]
        )
        filled_hexes = h3.polyfill_geojson(geojson=shape_to_feature(bounding_box)['geometry'], res=resolution)
    else:
        raise Exception(f"Unsupported geometry type: {poly_as_shape.geom_type}")

    # if polyfill returns empty, find the containing hex for the polygon centroid and start there
    if len(filled_hexes) == 0:
        filled_hexes = [h3.geo_to_h3(lat=poly_as_shape.centroid.y, lng=poly_as_shape.centroid.x, resolution=resolution)]

    # create dataframe of hexes and store them as geometry types
    hexes = pd.DataFrame(filled_hexes, columns=['id'])
    hexes['feature'] = hexes['id'].apply(hex_to_feature)
    hexes['shape'] = hexes['feature'].apply(lambda x: shape(x['geometry']))

    # check for full/partial intersection on the polyfill-generated hexes
    intersection_list = []
    do_neighbor_check = True
    for hex_as_shape in hexes['shape']:
        if poly_as_shape.contains(hex_as_shape):
            intersection_list.append('full')
        elif hex_as_shape.contains(poly_as_shape):
            intersection_list.append('partial')
            do_neighbor_check = False # if hex fully contains polygon, we can skip the neighbor check
        elif poly_as_shape.intersects(hex_as_shape):
            intersection_list.append('partial')
        else:
            intersection_list.append('none')

    hexes['intersection'] = intersection_list
    hexes['source'] = 'polyfill'

    # check the nearest neighbors of all partial-intersecting hexes.
    # if we find more partial-intersecting hexes, check those as well.
    # continue this process until no neighbors of intersecting hexes are left unchecked
    if do_neighbor_check:
        hexes = neighbor_check(hexes, poly_as_shape)

    return hexes

def neighbor_check(df:pd.DataFrame, poly_as_shape:shapely.geometry.shape) -> pd.DataFrame:
    """
    Intermediate function called by hex_intersections.

    Takes the list of polyfilled hexes and recursively checks for intersections between the neighbors of known hexes and the original polygon.
    """
    # check the neighbors of all partially-contained hexes
    hexes = df.copy()
    partials = hexes[hexes['intersection'].isin(['partial','full'])].copy()

    # a list of hexes whose neighbors will be checked for intersection with the polygon
    border_queue = partials['id'].copy().tolist()

    while len(border_queue):
        hex_id = border_queue[0]

        hex_neighbors = h3.k_ring(hex_id, 1)

        # find neighbors and calculate their intersections with the polygon
        for neighbor_id in hex_neighbors:
            if neighbor_id in hexes['id'].unique():
                # skip hexes we have already evaluated
                continue
            else:
                neighbor_as_feature = hex_to_feature(neighbor_id)
                neighbor_as_shape = shape(neighbor_as_feature['geometry'])
                
                # append as a partial neighbor if it intersects with the polygon
                if poly_as_shape.intersects(neighbor_as_shape):
                    hexes = pd.concat([
                        hexes,
                        pd.DataFrame([{
                            'id':neighbor_id,
                            'feature':neighbor_as_feature,
                            'shape':neighbor_as_shape,
                            'intersection':'partial',
                            'source':'neighbor'
                        }])
                    ])
                    border_queue.append(neighbor_id) # add new partial intersections to the border queue

                # append as a non-intersecting neighbor if no intersection with the polygon
                else:    
                    hexes = pd.concat([
                        hexes,
                        pd.DataFrame([{
                            'id':neighbor_id,
                            'feature':neighbor_as_feature,
                            'shape':neighbor_as_shape,
                            'intersection':'none',
                            'source':'neighbor'
                        }]
)
                    ])

        border_queue.remove(hex_id)

    return hexes


from bitarray.util import hex2ba, ba2int
class H3Bits:
    def __init__(self, hex):
        self.bits = hex2ba(hex)
    @property
    def reserved(self):
        return ba2int(self.bits[0:1])
    @property
    def index_mode(self):
        return ba2int(self.bits[1:5])
    @property
    def mode_dependent(self):
        return ba2int(self.bits[5:8])
    @property
    def resolution(self):
        return ba2int(self.bits[8:12])
    @property
    def base_cell(self):
        return ba2int(self.bits[12:19])
        
    @property
    def digit_1(self):
        return ba2int(self.bits[19:22])
    @property
    def digit_2(self):
        return ba2int(self.bits[22:25])
    @property
    def digit_3(self):
        return ba2int(self.bits[25:28])
    @property
    def digit_4(self):
        return ba2int(self.bits[28:31])
    @property
    def digit_5(self):
        return ba2int(self.bits[31:34])
    @property
    def digit_6(self):
        return ba2int(self.bits[34:37])
    @property
    def digit_7(self):
        return ba2int(self.bits[37:40])
    @property
    def digit_8(self):
        return ba2int(self.bits[40:43])
    @property
    def digit_9(self):
        return ba2int(self.bits[43:46])
    @property
    def digit_10(self):
        return ba2int(self.bits[46:49])
    @property
    def digit_11(self):
        return ba2int(self.bits[49:52])
    @property
    def digit_12(self):
        return ba2int(self.bits[52:55])
    @property
    def digit_13(self):
        return ba2int(self.bits[55:58])
    @property
    def digit_14(self):
        return ba2int(self.bits[58:61])
    @property
    def digit_15(self):
        return ba2int(self.bits[61:])
from shapely.geometry import Polygon, LineString, Point, GeometryCollection
from shapely.ops import split
from shapely.affinity import rotate

from pyproj import Geod
from numpy import arctan2, degrees
from math import hypot

import geojson
import json

def polygon_splitting_wrapper(feature, axis='minor', max_area=2.8, export_path=False, prints=False):
    """
    Take in a geojson feature and split it up into polygons until all are smaller than a max area

    Args:
        feature:        the geojson feature that we want to split up
        axis:           the axis on which we will split each feature (minor, vertical, horizontal)
        max_area:       the maximum area each polygon can be
                            defaults to 2.8 km^2 for OnSpot
        export_path:    export path of the final file (if exporting)
                            will export to a file based on the "name" property of the input feature
        prints:         binary flag to print info on the end result
    Returns:
        a geojson string FeatureCollection of polygons if export is false
        nothing if export is true, instead writing to a file

    Notes:
        only supports polygon features, not multipolys yet
    """
    # do the actual work
    splits = split_recursive(polygonify(feature), axis=axis, max_area=max_area)

    # if you want to print info on the end results, do that
    if prints:
        print(f"Feature got split into {len(splits.geoms)} polygons")
        print(f"Largest polygon in the feature collection is now:\n\t{max([polygon_area(poly) for poly in splits.geoms]):2.3} km^2")

    # grab the main feature name if it exists
    try:
        # use the feature name if possible
        main_name = feature['properties']['name']
    except:
        main_name = 'Polygon'

    # if we're gonna export it to a file, do that
    if export_path:
        geojson_stringify_collection(splits, main_name=main_name, export_path=export_path)
    else:
        return geojson_stringify_collection(splits, main_name=main_name)

def polygonify(feature):
    """
    Takes in a GeoJSON feature and spits it out as a shapely polygon

    Args:
        feature: geojson feature string or dictionary
    Returns:
        the feature as a shapely polygon
    """
    # make sure the feature is a dictionary
    feature = dictify_geojson(feature)

    return Polygon(feature['geometry']['coordinates'][0])

def dictify_geojson(feature):
    """
    Checks if the feature is a dictionary or not and converts if necessary

    Args:
        feature: feature to check
    Returns:
        feature as a dictionary
    """
    # if it's a dictionary, we're good
    if type(feature) == dict:
        return feature
    # convert strings
    elif type(feature) == str:
        return json.loads(feature)
    else:
        raise ValueError('Feature must be a string or dictionary')

def split_recursive(polygon, axis='minor', max_area=2.8):
    """
    recursively splits a shapely polygon into parts that are all smaller than the max_area

    Args:
        polygon:    shapely polygon to split up
        axis:       the type of split by axis (vertical, horizontal, minor)
        max_area:   the maximum area of each part
                        defaults to 2.8 km^2 or 2,800,000 m^2 
    Returns:
        shapely GeometryCollection of the split parts
    """

    # if this polygon is smaller than max area, coolio
    if polygon_area(polygon) <= max_area:
        return GeometryCollection(geoms = [polygon])
    else:
        # if it's too big, split it in half
        polys = split_polygon(polygon, axis=axis)
        # make an empty list for what we end up with
        split_geoms = []
        # for each of the split polys, redo this until they're all smaller than the max
        for poly in polys.geoms:
            # adding the new split parts to the existing polys for this chunk
            split_geoms = split_geoms + [split_poly for split_poly in split_recursive(poly, axis=axis, max_area=max_area).geoms]
        
        return GeometryCollection(split_geoms)

def polygon_area(polygon):
    """
    Finds the true area of a polygon

    Args:
        polygon: shapely polygon
    Returns:
        The area of that polygon in square kilometers
    """
    return abs(Geod(ellps="WGS84").geometry_area_perimeter(polygon)[0])/1e6

def azimuth(mrr):
    """
    Finds the azimuth of minimum_rotated_rectangle
    
    Args:
        mmr: minimum_rotated_rectangle of a shapely polygon
    """
    bbox  = list(mrr.exterior.coords)
    axis1 = _dist(bbox[0], bbox[3])
    axis2 = _dist(bbox[0], bbox[1])

    if axis1 <= axis2:
        az = _azimuth(bbox[0], bbox[1])
    else:
        az = _azimuth(bbox[0], bbox[3])

    return az

def _azimuth(point1, point2):
    """
    finds the azimuth between 2 points (interval 0 - 180)
    """

    angle = arctan2(point2[0] - point1[0], point2[1] - point1[1])
    return degrees(angle) if angle > 0 else degrees(angle) + 180

def _dist(a, b):
    """
    Calculates hypoteneuse distance between two points
    """

    return hypot(b[0] - a[0], b[1] - a[1])

def create_split_line(polygon, axis):
    """
    creates a LineString to split a polygon

    Args:
        polygon:    the shapely polygon we are going to split
        axis:       how we are going to split it
                        'vertical', 'horizontal', 'minor'
    Returns:
        shapely LineString across that split through the center of the polygon
    """
    # get the polygon bounds to make the splitter line where appropriate
    bounding = polygon.bounds

    if axis == 'vertical':
        return LineString([
            Point((bounding[0] +  bounding[2])/2, bounding[1]),
            Point((bounding[0] +  bounding[2])/2, bounding[3])
            ])
    elif axis == 'horizontal':
        return LineString([
            Point(bounding[0], (bounding[1] +  bounding[3])/2),
            Point(bounding[2], (bounding[1] +  bounding[3])/2)
            ])
    elif axis == 'minor':
        # minor axis is a bit wonky, but the coolest imo
        # create the horizontal line through the middle (and extend it a bit)
        line = LineString([
            Point(bounding[0]-0.1, (bounding[1] +  bounding[3])/2),
            Point(bounding[2]+0.1, (bounding[1] +  bounding[3])/2)
            ])

        # find the angle of the mmr
        # this negative is pretty important, who would've guessed?
        angle = -azimuth(polygon.minimum_rotated_rectangle)

        # return the horizontal line after rotating it to become the minor axis
        return rotate(line, angle, origin='center')

    else:
        raise ValueError("Axis must be 'vertical', 'horizontal', or 'minor.")

def split_polygon(polygon, axis):
    """
    Splits a shapely polygon roughly in half through the center
    
    Args:
        polygon:    shapely polygon to be split
        axis:       type of split (vertical, horizontal, or minor) axes
    Returns:
        the shapely GeometryCollection of the split parts
    """
    # get the LineString for however we're going to split it
    splitter = create_split_line(polygon, axis=axis)

    # split it into separate polygons
    return split(polygon, splitter)

def geojson_stringify_collection(geometry_collection, main_name="Polygon", export_path=False):
    """
    exports the final set of polygons into geojson. either a file or a string to do with what you will

    Args:
        geometry_collection: the shapely GeometryCollection of split parts
        export_name: the filename to export to if you want to export it
    Returns:
        dumped geojson string
        or nothing
    """

    if len(geometry_collection.geoms) > 1:
        GeoJSON_string = geojson.FeatureCollection(features=[geojson.Feature(geometry=poly, properties = {'name':f"{main_name}{ii+1}"}) for ii, poly in enumerate(geometry_collection.geoms)])
    else:
        GeoJSON_string = geojson.Feature(geometry=geometry_collection.geoms[0])

    if export_path:
        with open(f"{export_path}/{main_name}.geojson", 'w') as outfile:
            json.dump(GeoJSON_string, outfile, indent=4)
    else:
        return geojson.dumps(GeoJSON_string, indent=4)


from functools import partial
from pyproj import Proj, transform as pyproj_transform
from shapely.geometry import (
    shape,
    GeometryCollection,
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
    polygon,
    multipolygon,
)
from shapely.ops import transform as shapely_transform
from typing import Union
import numpy as np
import geojson
import shapely.wkb
import shapely.wkt

# Define type aliases
GeoJsonType: type = Union[geojson.Feature, geojson.FeatureCollection]
WkbType: type = Union[
    GeometryCollection,
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
]
WktType: type = WkbType


def wkb2geojson(wkb: bytes) -> GeoJsonType:
    """
    Converts Well-Known Binary (WKB) geometry to GeoJSON format.

    Parameters
    ----------
    wkb : bytes
        Well-Known Binary (WKB) representation of the geometry.

    Returns
    -------
    GeoJsonType
        The converted geometry in GeoJSON format.

    Notes
    -----
    This function converts a WKB geometry to GeoJSON format. It uses the `shapely.wkb.loads` function to load the WKB geometry,
    validates the geometry, and constructs the GeoJSON representation using the `geojson.Feature` or `geojson.FeatureCollection` class.
    If the WKB geometry is a `GeometryCollection`, it creates multiple features for each geometry in the collection and returns a `geojson.FeatureCollection`.
    Otherwise, it creates a single feature with the geometry and returns a `geojson.Feature`.
    """
    wkb = shapely.wkb.loads(wkb)
    wkb = validate_wkb(wkb)
    if type(wkb) == GeometryCollection:
        features = [geojson.Feature(geometry=o, properties={}) for o in wkb.geoms]
        feature_collection = geojson.FeatureCollection(features)
        return feature_collection
    else:
        feature = geojson.Feature(geometry=wkb, properties={})
        return feature


def wkt2geojson(wkt: str) -> GeoJsonType:
    """
    Converts Well-Known Text (WKT) geometry to GeoJSON format.

    Parameters
    ----------
    wkt : str
        Well-Known Text (WKT) representation of the geometry.

    Returns
    -------
    GeoJsonType
        The converted geometry in GeoJSON format.

    Notes
    -----
    This function converts a WKT geometry to GeoJSON format. It uses the `shapely.wkt.loads` function to load the WKT geometry,
    validates the geometry, and constructs the GeoJSON representation using the `geojson.Feature` or `geojson.FeatureCollection` class.
    If the WKT geometry is a `GeometryCollection`, it creates multiple features for each geometry in the collection and returns a `geojson.FeatureCollection`.
    Otherwise, it creates a single feature with the geometry and returns a `geojson.Feature`.
    """
    wkt = shapely.wkt.loads(wkt)
    wkt = validate_wkt(wkt)
    if type(wkt) == GeometryCollection:
        features = [geojson.Feature(geometry=o, properties={}) for o in wkt.geoms]
        feature_collection = geojson.FeatureCollection(features)
        return feature_collection
    else:
        feature = geojson.Feature(geometry=wkt, properties={})
        return feature


def geojson2shape(
    data: Union[GeoJsonType, dict]
) -> Union[GeometryCollection, Polygon, MultiPolygon,]:
    """
    Converts GeoJSON format to Shapely geometry objects.

    Parameters
    ----------
    data : Union[GeoJsonType, dict]
        The GeoJSON geometry or dictionary representing the GeoJSON geometry.

    Returns
    -------
    Union[GeometryCollection, Polygon, MultiPolygon]
        The converted Shapely geometry.

    Notes
    -----
    This function converts GeoJSON format to Shapely geometry objects. It validates the input GeoJSON using the `validate_geojson` function,
    and constructs the Shapely geometry objects using the `shape` function. If the GeoJSON is a `Polygon`, it orients the polygon with a positive sign.
    If the GeoJSON is a `MultiPolygon`, it orients each polygon in the collection with a positive sign. If the GeoJSON is a `Feature`,
    it extracts the geometry and converts it to Shapely format. If the GeoJSON is a `FeatureCollection`, it iterates over the features,
    converts each geometry to Shapely format, and returns a `GeometryCollection`.
    """
    data = validate_geojson(data)
    if data.get("type") == "Polygon":
        return polygon.orient(shape(data), sign=1.0)
    elif data.get("type") == "MultiPolygon":
        return MultiPolygon(
            [polygon.orient(poly, sign=1.0) for poly in shape(data).geoms]
        )
    elif data.get("type") == "Feature":
        return geojson2shape(data.get("geometry"))
    elif data.get("type") == "FeatureCollection":
        g = [geojson2shape(f.get("geometry")) for f in data.get("features")]
        return GeometryCollection(g)
    else:
        raise Exception


def geojson2wkb(data: Union[GeoJsonType, dict]) -> WktType:
    """
    Converts GeoJSON format to Well-Known Binary (WKB) representation.

    Parameters
    ----------
    data : Union[GeoJsonType, dict]
        The GeoJSON geometry or dictionary representing the GeoJSON geometry.

    Returns
    -------
    WktType
        The converted Well-Known Binary (WKB) representation.

    Notes
    -----
    This function converts GeoJSON format to Well-Known Binary (WKB) representation. It validates the input GeoJSON using the `validate_geojson` function,
    converts the GeoJSON to Shapely format using the `geojson2shape` function, and retrieves the WKB representation of the Shapely geometry using the `.wkb` attribute.
    """
    return geojson2shape(validate_geojson(data)).wkb


def geojson2wkt(data: Union[GeoJsonType, dict]) -> WktType:
    """
    Converts GeoJSON format to Well-Known Text (WKT) representation.

    Parameters
    ----------
    data : Union[GeoJsonType, dict]
        The GeoJSON geometry or dictionary representing the GeoJSON geometry.

    Returns
    -------
    WktType
        The converted Well-Known Text (WKT) representation.

    Notes
    -----
    This function converts GeoJSON format to Well-Known Text (WKT) representation. It validates the input GeoJSON using the `validate_geojson` function,
    converts the GeoJSON to Shapely format using the `geojson2shape` function, and retrieves the WKT representation of the Shapely geometry using the `.wkt` attribute.
    """
    return geojson2shape(validate_geojson(data)).wkt


def validate_geojson(data: Union[GeoJsonType, dict]) -> GeoJsonType:
    """
    Validates the GeoJSON format.

    Parameters
    ----------
    data : Union[GeoJsonType, dict]
        The GeoJSON geometry or dictionary representing the GeoJSON geometry.

    Returns
    -------
    GeoJsonType
        The validated GeoJSON geometry.

    Notes
    -----
    This function validates the GeoJSON format using the `geojson.loads` and `geojson.dumps` functions.
    It ensures that the GeoJSON is valid and returns the validated GeoJSON object.
    """
    data = geojson.loads(geojson.dumps(data))
    if not data.is_valid:
        return None

    return data


def validate_wkb(data: WkbType) -> WkbType:
    """
    Validates the Well-Known Binary (WKB) representation.

    Parameters
    ----------
    data : WkbType
        The Well-Known Binary (WKB) representation of the geometry.

    Returns
    -------
    WkbType
        The validated Well-Known Binary (WKB) representation.

    Notes
    -----
    This function validates the Well-Known Binary (WKB) representation using the `.is_valid` attribute of the Shapely geometry object.
    It ensures that the WKB representation is valid and returns the validated WKB object.
    """
    if not data.is_valid:
        return None
    return data


def validate_wkt(data: WktType) -> WktType:
    """
    Validates the Well-Known Text (WKT) representation.

    Parameters
    ----------
    data : WktType
        The Well-Known Text (WKT) representation of the geometry.

    Returns
    -------
    WktType
        The validated Well-Known Text (WKT) representation.

    Notes
    -----
    This function validates the Well-Known Text (WKT) representation using the `.is_valid` attribute of the Shapely geometry object.
    It ensures that the WKT representation is valid and returns the validated WKT object.
    """
    if not data.is_valid:
        return None
    return data


def latlon_buffer(lat: float, lon: float, radius: int, cap_style=int):
    """
    Create a buffer circle around a latitude-longitude point with a given radius in meters.

    Parameters
    ----------
    lat : float
        The latitude of the center point.
    lon : float
        The longitude of the center point.
    radius : int
        The radius of the buffer circle in meters.
    cap_style : int, optional
        The style of the buffer cap. Possible values are:
            - round: 1
            - flat: 2
            - square: 3
        Default is `int`.

    Returns
    -------
    shapely.geometry.Polygon
        The buffered polygon representing the circle.

    Notes
    -----
    This function creates a buffer circle around a latitude-longitude point using the Shapely library.
    It transforms the coordinates to an azimuthal equidistant projection centered at the given point,
    creates a buffer using the projected coordinates, and then transforms the buffer back to latitude-longitude coordinates.
    """

    local_azimuthal_projection = (
        "+proj=aeqd +R=6371000 +units=m +lat_0={} +lon_0={}".format(lat, lon)
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

    point_transformed = shapely_transform(wgs84_to_aeqd, Point(float(lon), float(lat)))
    buffer = point_transformed.buffer(radius, cap_style=cap_style)
    # Get the polygon with lat lon coordinates
    buffered_poly = shapely_transform(aeqd_to_wgs84, buffer)
    return buffered_poly


def points_in_poly_numpy(x: np.array, y: np.array, poly: np.array) -> np.array:
    """
    Find points that are within a given polygon.

    Parameters
    ----------
    x : numpy.ndarray
        The x-coordinates of the points to check.
    y : numpy.ndarray
        The y-coordinates of the points to check.
    poly : numpy.ndarray
        The coordinates of the polygon vertices.

    Returns
    -------
    numpy.ndarray
        A boolean array with the same length as the input points.
        True if the point is inside the polygon, False otherwise.

    Notes
    -----
    This function uses numpy vectorization to efficiently check if points are inside a polygon.
    It iterates through the polygon vertices and checks if each point is within the bounds of the corresponding polygon segment.
    The function returns a boolean array indicating whether each point is inside the polygon or not.
    """

    n = len(poly)

    # initialize an array for returns
    inside = np.zeros(len(x), np.bool_)

    # and set up some 'blank' variables we'll use later
    p2x = 0.0
    p2y = 0.0
    xints = 0.0

    # start out with the first point on the polygon
    p1x, p1y = poly[0]

    # loop through the polygon coordinates
    for i in range(1, n + 1):
        # modulo the polygon coordinates since we need it to close back on the first one
        p2x, p2y = poly[i % n]

        # check if the point is within the y-bounds and not greater than the max x of the poly coordinates
        idx = np.nonzero(
            (y > min(p1y, p2y)) & (y <= max(p1y, p2y)) & (x <= max(p1x, p2x))
        )[0]

        # if the point is in that region
        if len(idx):
            # if the two polygon coords aren't in a horizontal line
            if p1y != p2y:
                # find the x where the point projects horizontally onto the line between polygon coordinates
                xints = (y[idx] - p1y) * (p2x - p1x) / (p2y - p1y) + p1x

            # if the two polygon coords are in a vertical line
            if p1x == p2x:
                # the point then falls on the line between the two points and is counted (edge inclusion)
                inside[idx] = ~inside[idx]
            else:
                # check if the x point is to left of the intercept and count those that are
                idxx = idx[x[idx] <= xints]
                inside[idxx] = ~inside[idxx]

        # set p1 to the next polygon coordinate
        p1x, p1y = p2x, p2y

    return inside

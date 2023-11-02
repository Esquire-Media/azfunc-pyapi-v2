import numpy as np

def auto_zoom_level(geometries, fig_width=600, fig_height=600, margin=1.5):
    """
    Calculates the plotly zoom level that will show all geometries with limited margin.

    Inputs:
    * geometries:   geopandas series of the geometries. e.g. df['geometry']
    * aspect_ratio: width/height of final map
    * margin:       a padding factor for the boundaries.

    Outputs:
    * zoom:         a plotly zoom level
    """
    aspect_ratio = fig_width / fig_height

    # if passing polygons
    if 'GeoJSON' in geometries.columns:
        minlat = 180
        minlon = 180
        maxlat = -180
        maxlon = -180
        # get bounding coordinates
        for js in geometries['GeoJSON']:
        
            # find coordinates list for Polygon and Multipolygon types
            if js['type'] == 'Polygon':
                coords = js['coordinates'][0]
            if js['type'] == 'MultiPolygon':
                coords = [coord for feature in js['coordinates'] for coord in feature[0]]

            for coord in coords:
                if coord[1] < minlat:
                    minlat = coord[1]
                if coord[0] < minlon:
                    minlon = coord[0]
                if coord[1] > maxlat:
                    maxlat = coord[1]
                if coord[0] > maxlon:
                    maxlon = coord[0]
    # if passing points
    elif 'Latitude' in geometries.columns and 'Longitude' in geometries.columns:
        minlat = geometries['Latitude'].min()
        minlon = geometries['Longitude'].min()
        maxlat = geometries['Latitude'].max()
        maxlon = geometries['Longitude'].max()
    else:
        raise Exception('Auto-Zoom-Level was unable to detect either a "GeoJSON" column or "Latitude" and "Longitude" columns.')

    center = [(minlat+maxlat)/2, (minlon+maxlon)/2]

    # find the lat/lon ranges that we want to show for the geometries we're plotting
    height = abs(maxlat - minlat) * margin 
    width  = abs(maxlon - minlon) * margin * aspect_ratio

    # convert height and width to meters
    height = 111000 * height
    width  = 111000 * np.cos(np.deg2rad(np.mean([maxlat,minlat]))) * width

    # interpolation setup of meters/pixel at various zoom levels
    lat_meters_per_pixel = np.array([
        0.019,0.037,0.075,0.149,0.299,0.597,1.194,2.389,4.777,9.555,19.109,38.218,76.437,152.874,
        305.748,611.496,1222.992,2445.984,4891.968,9783.936,19567.871,39135.742,78271.484,
    ])
    lon_meters_per_pixel = lat_meters_per_pixel * np.cos(np.deg2rad(np.mean([maxlat,minlat])))
    met_zoom_lev = range(22, -1, -1)

    lon_zoom = np.interp(width/fig_width,  lon_meters_per_pixel, met_zoom_lev)
    lat_zoom = np.interp(height/fig_height, lat_meters_per_pixel, met_zoom_lev)

    # find which, lat or lon, is the less restrictive zoom
    zoom = round(min(lon_zoom, lat_zoom), 2)

    return zoom, center
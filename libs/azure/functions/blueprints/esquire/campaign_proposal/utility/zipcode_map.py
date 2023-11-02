import numpy as np
import pandas as pd
import plotly.express as px
import plotly.io as pio
import json
import logging
import os
from libs.utils.plots import auto_zoom_level

def map_zipcodes(zips:pd.DataFrame, mapbox_token:str, export_path:str=None, return_bytes:bool=False):
    """
    Exports a map of zipcodes.
    """
    # create a geojson collection of zipcode features to use
    zips_dict = {'type':'FeatureCollection','features':[]}
    for i, row in zips.iterrows():
        js = {
            'type':'Feature',
            'geometry':row['GeoJSON']
        }
        js['id'] = i
        zips_dict['features'].append(js)

    # calculate viewport center and set the zoom level
    zoom, center = auto_zoom_level(geometries=zips, fig_width=269, fig_height=205)

    # set up the main polygon figure
    fig = px.choropleth_mapbox(
        zips,
        geojson      = zips_dict,
        locations    = zips.index,
        mapbox_style = "carto-positron",
        zoom         = zoom, 
        # center on the midpoint lat lon of the zipcodes
        center       = { 
            "lat" : center[0],
            "lon" : center[1]
            },
        color_discrete_sequence=['#F15A29'],
        opacity      = 0.8,
        width        = 269, 
        height       = 205,
        hover_name   = 'Zipcode',
    )

    # turn off colorbar legend
    fig.update_coloraxes(showscale=False)

    # mapbox styling
    fig.update_layout(
        mapbox_style        = "dark", 
        mapbox_accesstoken  = mapbox_token,
        showlegend          = False,
        margin              = {"r":0,"t":0,"l":0,"b":0},
    )

    if return_bytes:
        return pio.to_image(fig)
    if export_path != None:
        fig.write_image(export_path)
    else:
        fig.show()
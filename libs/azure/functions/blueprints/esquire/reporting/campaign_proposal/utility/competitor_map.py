import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import pandas as pd
import os
from libs.utils.plots import auto_zoom_level

def map_competitors(comps:pd.DataFrame, owned:pd.DataFrame, mapbox_token:str, export_path:str=None, return_bytes:bool=False):
    """
    Exports a map of competitors and owned locations.
    """

    # calculate viewport center and set the zoom level
    fig_width = 572*1.5
    fig_height = 300*1.5
    zoom, center = auto_zoom_level(comps.rename(columns={'latitude':'Latitude','longitude':'Longitude'}), fig_width=fig_width, fig_height=fig_height)

    # trace just for camera settings
    fig = px.scatter_mapbox(
        comps[:0],
        lat = 'latitude',
        lon = 'longitude',
        zoom = zoom, 
        # center on the midpoint lat lon
        center = { 
            "lat" : center[0],
            "lon" : center[1]
            },
        width = fig_width, 
        height = fig_height,
    )

    # graph the competitor locations
    fig.add_scattermapbox(
        name='Competitors',
        lat = comps['latitude'],
        lon = comps['longitude'],
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=10,
            color='#0B5394',
            opacity=1,
        ),
    )

    # graph the owned locations
    fig.add_scattermapbox(
        name='Owned Locations',
        lat = owned['latitude'],
        lon = owned['longitude'],
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=10,
            color='#F15A29',
            opacity=1,
        ),
    )

    # turn off colorbar legend
    fig.update_coloraxes(showscale=False)

    # mapbox styling
    fig.update_layout(
        mapbox_style        = "light", 
        mapbox_accesstoken  = mapbox_token,
        margin              = {"r":0,"t":0,"l":0,"b":0},
    )
    fig.update_layout(legend=dict(
        yanchor="bottom",
        y=0.01,
        xanchor="right",
        x=0.99
    ))
    if return_bytes:
        return pio.to_image(fig)
    if export_path != None:
        fig.write_image(export_path)
    else:
        fig.show()
from typing import List, Tuple

import geopandas as gpd
import networkx as nx
import numpy as np
import plotly.express as px
import plotly.graph_objs as go
from cloud_utils import upload_to_gcs
from google.cloud.storage import Bucket


def plot_points(
    points: List[Tuple[float, float]],
    mapbox_access_token: str,
    output_path: str,
    color: str,
    bucket: Bucket,
) -> None:
    """Plots points on a Mapbox map, and saves the created HTML to GCS"""

    # Retrieve latitudes and longitudes
    lats = [nl[0] for nl in points]
    lons = [nl[1] for nl in points]

    # Get the map center:
    x_center = (min(lons) + max(lons)) / 2
    y_center = (min(lats) + max(lats)) / 2

    fig = go.Figure(
        go.Scattermapbox(  # trace for nodes
            lat=lats,
            lon=lons,
            mode="markers",
            marker=dict(size=8, colorscale="matter", color=color),
            showlegend=False,
        )
    )

    fig.update_layout(
        autosize=True,
        mapbox=dict(
            accesstoken=mapbox_access_token,
            bearing=0,
            center=dict(lat=y_center, lon=x_center + 0.01),
            pitch=0,
            zoom=12,
        ),
    )

    # Export figure to HTML text and save to cloud
    figure_text = fig.to_html()
    upload_to_gcs(bucket, figure_text, output_path, content_type="text/html")


def plot_graph(
    g: nx.Graph, mapbox_access_token: str, output_path: str, color: str, bucket: Bucket
) -> None:
    """Plots graph with nodes and edges, on a Mapbox map, and saves the created HTML to GCS"""

    # Retrieve nodes ids, latitudes and longitudes
    node_data = list(g.nodes(data=True))
    ids = [nl[0] for nl in node_data]
    lats = [nl[1]["lat"] for nl in node_data]
    lons = [nl[1]["lon"] for nl in node_data]

    # Define the dict for the graph layout (i.e. node geographic locations):
    pos = {i: [lo, la] for i, lo, la in zip(ids, lons, lats)}

    # Get the map center:
    x_center = (min(lons) + max(lons)) / 2
    y_center = (min(lats) + max(lats)) / 2

    fig = go.Figure(
        go.Scattermapbox(  # trace for nodes
            lat=lats,
            lon=lons,
            mode="markers",
            marker=dict(size=8, colorscale="matter", color=color),
            showlegend=False,
        )
    )

    # Retrieve edges
    edge_list = list(g.edges(data=True))
    pl_edges = [(item[0], item[1]) for item in edge_list]

    # Define coordinates of the edge ends as `'MultiLineString'` data type, see https://en.wikipedia.org/wiki/GeoJSON
    coords = []
    for e in pl_edges:
        coords.append([[pos[e[0]][0], pos[e[0]][1]], [pos[e[1]][0], pos[e[1]][1]]])

    # Define the Mapbox layers representing the graph edges:
    layers = [
        dict(
            sourcetype="geojson",
            source={
                "type": "Feature",
                "geometry": {"type": "MultiLineString", "coordinates": coords},
            },
            color=color,
            type="line",
            line=dict(width=1.5),
        )
    ]
    fig.update_layout(
        autosize=True,
        mapbox=dict(
            accesstoken=mapbox_access_token,
            layers=layers,
            bearing=0,
            center=dict(lat=y_center, lon=x_center + 0.01),
            pitch=0,
            zoom=15,
        ),
    )

    # Export figure to HTML text and save to cloud
    figure_text = fig.to_html()
    upload_to_gcs(bucket, figure_text, output_path, content_type="text/html")


def plot_card(gdf: gpd.GeoDataFrame, output_path: str, bucket: Bucket) -> None:

    # Define center of map
    center_lat = gdf.geometry.centroid.y.mean()
    center_lon = gdf.geometry.centroid.x.mean()

    # Define years and coverage thresholds
    years = sorted(gdf["years"].unique())
    thresholds = np.arange(0, 101, 5)

    # Create figure
    fig = go.Figure()

    # Add traces for cumulative years and thresholds
    for year in years:
        cumulative_gdf = gdf[
            gdf["years"] == year
        ]  # Include all years up to the current year
        for threshold in thresholds:
            filtered_gdf = cumulative_gdf[cumulative_gdf["coverage"] >= threshold]
            fig.add_trace(
                go.Choroplethmapbox(
                    geojson=filtered_gdf.geometry.__geo_interface__,
                    locations=filtered_gdf.index,
                    z=filtered_gdf["cumulative_percentage"],
                    colorscale="Viridis",
                    zmin=0,
                    zmax=100,
                    marker_opacity=0.5,
                    hoverinfo="text",
                    text=filtered_gdf.apply(
                        lambda row: f"Zone: {row['name']}<br>GSV coverage: {row['coverage']}%<br>GSV total km: {row['total_sv']}<br>OSM total km: {row['total_osm']}<br>Cumulative dates: {row['cumulative_percentage']}%",
                        axis=1,
                    ),
                )
            )

            # Make only the first year and first threshold trace visible
            fig.data[-1].visible = (year == years[0]) and (threshold == thresholds[0])

    # Create year slider steps
    year_steps = []
    for year_idx, year in enumerate(years):
        step = dict(
            method="update",
            args=[
                {
                    "visible": [
                        (trace_idx // len(thresholds) == year_idx)
                        for trace_idx in range(len(fig.data))
                    ]
                },
                {"title": f"Cumulative Year: {year}"},
            ],
            label=f"{year}",
        )
        year_steps.append(step)

    # Create coverage slider steps
    coverage_steps = []
    for threshold_idx, threshold in enumerate(thresholds):
        step = dict(
            method="update",
            args=[
                {
                    "visible": [
                        (trace_idx % len(thresholds) == threshold_idx)
                        for trace_idx in range(len(fig.data))
                    ]
                },
                {"title": f"Coverage Threshold: {threshold}%"},
            ],
            label=f"{threshold}",
        )
        coverage_steps.append(step)

    # Add sliders to layout
    year_slider = dict(
        active=0,
        currentvalue={"prefix": "Years intervals: "},
        pad={"t": 50},
        steps=year_steps,
    )

    coverage_slider = dict(
        active=0,
        currentvalue={"prefix": "Coverage Threshold: "},
        pad={"t": 110},
        steps=coverage_steps,
    )

    fig.update_layout(
        mapbox_style="open-street-map",
        mapbox_zoom=13,
        mapbox_center={"lat": center_lat, "lon": center_lon},
        sliders=[year_slider, coverage_slider],
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )

    # Export figure to HTML text and save to cloud
    figure_text = fig.to_html()
    upload_to_gcs(bucket, figure_text, output_path, content_type="text/html")

import argparse
import json

import geopandas as gpd
import plotly.graph_objects as go
from pyproj import Geod
from shapely.geometry import MultiPolygon, Polygon


def compute_polygon_area(geometry):
    """
    Compute the area of a geometry (Polygon or MultiPolygon) defined by latitude/longitude coordinates.

    Args:
        geometry (Polygon or MultiPolygon): Geometry object.

    Returns:
        float: Area in square kilometers.
    """
    geod = Geod(ellps="WGS84")
    total_area_m2 = 0

    # Handle MultiPolygon or Polygon
    if isinstance(geometry, Polygon):
        total_area_m2 = abs(geod.geometry_area_perimeter(geometry)[0])
    elif isinstance(geometry, MultiPolygon):
        for polygon in geometry.geoms:
            total_area_m2 += abs(geod.geometry_area_perimeter(polygon)[0])
    else:
        raise ValueError(
            "Unsupported geometry type. Only Polygon and MultiPolygon are supported."
        )

    return total_area_m2 / 1_000_000  # Convert to square kilometers


def display_senegal(shapefile_path: str) -> None:
    gdf = gpd.read_file(shapefile_path)
    gdf = gdf.to_crs("EPSG:4326")

    columns_renamed = ["arrondissment", "department", "region"]
    columns = ["ADM3_FR", "ADM2_FR", "ADM1_FR"]
    gdf = gdf.rename(columns=dict(zip(columns, columns_renamed)))

    for column in columns_renamed:
        gdf.loc[:, column] = (
            gdf[column]
            .astype(str)
            .str.lower()
            .apply(lambda x: x.replace(" ", "_").replace("n/a", "unknown"))
        )

    # Drop unwanted columns and calculate area in square kilometers if needed
    gdf = gdf[columns_renamed + ["geometry", "Shape_Area"]]

    regions_gdf = gdf.dissolve(by="region", as_index=False)
    departments_gdf = gdf.dissolve(by="department", as_index=False)

    # Convert GeoDataFrames to GeoJSON format for Plotly
    arrondissments_geojson = json.loads(gdf.to_json())
    regions_geojson = json.loads(regions_gdf.to_json())
    departments_geojson = json.loads(departments_gdf.to_json())

    # Define a helper function to create a choropleth layer
    def create_choropleth_layer(
        geojson, locations, feature_id_key, hover_column, color_column
    ):
        return go.Choroplethmapbox(
            geojson=geojson,
            featureidkey=feature_id_key,
            locations=locations,
            z=[feature["properties"][color_column] for feature in geojson["features"]],
            text=[
                feature["properties"][hover_column] for feature in geojson["features"]
            ],
            colorscale="Viridis",
            colorbar_title="Area (km²)",
            marker_opacity=0.6,
            marker_line_width=0,
        )

    # Create initial figure with one layer
    fig = go.Figure()

    # Add a choropleth layer for each geographic granularity
    fig.add_trace(
        create_choropleth_layer(
            arrondissments_geojson,
            gdf["arrondissment"],
            "properties.arrondissment",
            "arrondissment",
            "Shape_Area",
        )
    )
    fig.add_trace(
        create_choropleth_layer(
            departments_geojson,
            departments_gdf["department"],
            "properties.department",
            "department",
            "Shape_Area",
        )
    )
    fig.add_trace(
        create_choropleth_layer(
            regions_geojson,
            regions_gdf["region"],
            "properties.region",
            "region",
            "Shape_Area",
        )
    )

    # Set up layout
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=5,
        mapbox_center={"lat": 9.1, "lon": 3.5},
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )

    # Update visibility using dropdown menu
    fig.update_traces(visible=False)
    fig.data[0].visible = True  # Start with wards visible

    # Add dropdown menu for selecting geographic granularity
    fig.update_layout(
        updatemenus=[
            {
                "buttons": [
                    {
                        "label": "Arrondissment Level",
                        "method": "update",
                        "args": [
                            {"visible": [True, False, False]},
                            {"title": "Arrondissment Areas in Senegal"},
                        ],
                    },
                    {
                        "label": "Department Level",
                        "method": "update",
                        "args": [
                            {"visible": [False, True, False]},
                            {"title": "Department Areas in Senegal"},
                        ],
                    },
                    {
                        "label": "Region Level",
                        "method": "update",
                        "args": [
                            {"visible": [False, False, True]},
                            {"title": "Region Areas in Senegal"},
                        ],
                    },
                ],
                # "direction": "down",
                "showactive": True,
                "x": 0.1,
                "xanchor": "left",
                "y": 1.15,
                "yanchor": "top",
                "active": 0,
            }
        ]
    )

    config = {"scrollZoom": True}
    fig.write_html("senegal_map.html", config=config)


def display_nigeria(shapefile_path: str) -> None:
    gdf = gpd.read_file(shapefile_path)
    gdf = gdf.to_crs("EPSG:4326")

    columns = ["wardname", "lganame", "statename"]
    for column in columns:
        gdf.loc[:, column] = (
            gdf[column]
            .astype(str)
            .str.lower()
            .apply(lambda x: x.replace(" / ", "-").replace(" ", "_").replace("/", "-"))
        )

    # Drop unwanted columns and calculate area in square kilometers if needed
    gdf = gdf.drop(columns=["timestamp"])
    gdf = gdf[gdf["status"] != "Invalid"]
    gdf["area_km2"] = gdf["geometry"].apply(compute_polygon_area)
    states_gdf = gdf.dissolve(by="statename", as_index=False)
    states_gdf["area_km2"] = states_gdf["geometry"].apply(compute_polygon_area)
    lgas_gdf = gdf.dissolve(by="lganame", as_index=False)
    lgas_gdf["area_km2"] = lgas_gdf["geometry"].apply(compute_polygon_area)

    # Convert GeoDataFrames to GeoJSON format for Plotly
    wards_geojson = json.loads(gdf.to_json())
    lgas_geojson = json.loads(lgas_gdf.to_json())
    states_geojson = json.loads(states_gdf.to_json())

    # Define a helper function to create a choropleth layer
    def create_choropleth_layer(
        geojson, locations, feature_id_key, hover_column, color_column
    ):
        return go.Choroplethmapbox(
            geojson=geojson,
            featureidkey=feature_id_key,
            locations=locations,
            z=[feature["properties"][color_column] for feature in geojson["features"]],
            text=[
                feature["properties"][hover_column] for feature in geojson["features"]
            ],
            colorscale="Viridis",
            colorbar_title="Area (km²)",
            marker_opacity=0.6,
            marker_line_width=0,
        )

    # Create initial figure with one layer
    fig = go.Figure()

    # Add a choropleth layer for each geographic granularity
    fig.add_trace(
        create_choropleth_layer(
            wards_geojson, gdf["FID"], "properties.FID", "wardname", "area_km2"
        )
    )
    fig.add_trace(
        create_choropleth_layer(
            lgas_geojson,
            lgas_gdf["lganame"],
            "properties.lganame",
            "lganame",
            "area_km2",
        )
    )
    fig.add_trace(
        create_choropleth_layer(
            states_geojson,
            states_gdf["statename"],
            "properties.statename",
            "statename",
            "area_km2",
        )
    )

    # Set up layout
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=5,
        mapbox_center={"lat": 9.1, "lon": 3.5},
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )

    # Update visibility using dropdown menu
    fig.update_traces(visible=False)
    fig.data[0].visible = True  # Start with wards visible

    # Add dropdown menu for selecting geographic granularity
    fig.update_layout(
        updatemenus=[
            {
                "buttons": [
                    {
                        "label": "Ward Level",
                        "method": "update",
                        "args": [
                            {"visible": [True, False, False]},
                            {"title": "Ward Areas in Nigeria"},
                        ],
                    },
                    {
                        "label": "LGA Level",
                        "method": "update",
                        "args": [
                            {"visible": [False, True, False]},
                            {"title": "LGA Areas in Nigeria"},
                        ],
                    },
                    {
                        "label": "State Level",
                        "method": "update",
                        "args": [
                            {"visible": [False, False, True]},
                            {"title": "State Areas in Nigeria"},
                        ],
                    },
                ],
                # "direction": "down",
                "showactive": True,
                "x": 0.1,
                "xanchor": "left",
                "y": 1.15,
                "yanchor": "top",
                "active": 0,
            }
        ]
    )

    fig.write_html("nigeria_map.html")


def parse_arguments():
    """
    Parse command-line arguments for country and shapefile path.

    Returns:
        argparse.Namespace: Parsed arguments with attributes `country` and `shapefile`.
    """
    parser = argparse.ArgumentParser(
        description="Display geographic data for a specified country."
    )
    parser.add_argument(
        "--country",
        type=str,
        required=True,
        choices=["senegal", "nigeria"],
        help="The country to display (e.g., 'senegal' or 'nigeria').",
    )
    parser.add_argument(
        "--shapefile",
        type=str,
        required=True,
        help="The local path to the shapefile. Be aware that multiple shapefiles are needed for a proper read",
    )
    return parser.parse_args()


if __name__ == "__main__":
    # Parse command-line arguments
    args = parse_arguments()

    if args.country == "senegal":
        display_senegal(args.shapefile)
    elif args.country == "nigeria":
        display_nigeria(args.shapefile)

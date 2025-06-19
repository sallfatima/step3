import os
from typing import List, Optional, Tuple

import geopandas as gpd
import networkx.readwrite.json_graph as json_graph
from build_utils import remove_nodes_not_part_of_polygon
from cloud_utils import read_json_gcs, upload_json_to_gcs
from config_model import SetupConfig
from general_utils import reverse_lat_lon
from google.cloud.storage import Bucket
from logger import logger
from omegaconf import ListConfig
from polygon_reader_utils import read_region_polygons
from shapely import Polygon
from shapely.geometry import MultiPoint
from viz_utils import plot_graph


def check_broader_area(cfg: SetupConfig, bucket: Bucket) -> bool:
    """
    Checks if a broader region was already computed and decide if the pipeline should continue or not.
    If the pipeline does not continue, this means that the OSM and SV maps were computed from the broader area.
    Args:
        cfg: configuration dictionary
        bucket: GCS bucket

    Returns:
        a boolean indicating whether the pipeline should continue or not
    """

    def search(start_search_path: str) -> bool:
        """Returns a boolean that specifies if the search should continue"""

        # Define broader region
        broader_region = os.path.dirname(start_search_path)
        broader_region_data = f"{cfg.database_path}/{broader_region}/{os.path.basename(cfg.area.data_path)}"

        # Search for broader region files
        broader_region_data_osm = f"{broader_region_data}/{cfg.osm_name}_merged.json"
        broader_region_data_sv = f"{broader_region_data}/{cfg.sv_name}_merged.json"

        osm_graph_exists = bucket.blob(broader_region_data_osm).exists()
        sv_graph_exists = bucket.blob(broader_region_data_sv).exists()

        # If the broader region files exist
        if osm_graph_exists and sv_graph_exists:

            # Get current polygon
            current_polygon = Polygon(cfg.area.polygon)

            # Read graph for broader OSM
            osm_data = read_json_gcs(bucket, broader_region_data_osm)
            g = json_graph.adjacency_graph(osm_data)

            # Extract the positions of the nodes
            positions = [[data["lat"], data["lon"]] for _, data in g.nodes(data=True)]
            # Create a MultiPoint object from the positions
            points = MultiPoint(positions)
            # Get the convex hull of the points (enclosing polygon)
            broader_polygon = points.convex_hull

            # Check that broader polygon fully contain the current region
            if broader_polygon.contains(current_polygon):

                # Keep only nodes part of current window
                g = remove_nodes_not_part_of_polygon(g, current_polygon)
                json_data = json_graph.adjacency_data(g)

                # Save data
                osm_graph_path = f"{cfg.area.data_path}/{cfg.osm_name}_merged.json"
                upload_json_to_gcs(bucket, osm_graph_path, json_data)

                if cfg.viz and g.number_of_nodes() > 0:
                    output_map_osm_path = (
                        f"{cfg.area.viz_path}/{cfg.osm_name}_merged.html"
                    )
                    plot_graph(g, cfg.mapbox_token, output_map_osm_path, "blue", bucket)

                # Delete osm data from memory
                del osm_data

                # Read broader SV data
                sv_data = read_json_gcs(bucket, broader_region_data_sv)
                g = json_graph.adjacency_graph(sv_data)

                # Keep only nodes part of current window
                g = remove_nodes_not_part_of_polygon(g, current_polygon)
                json_data = json_graph.adjacency_data(g)

                # Save data
                sv_graph_path = f"{cfg.area.data_path}/{cfg.sv_name}_merged.json"
                upload_json_to_gcs(bucket, sv_graph_path, json_data)

                if cfg.viz and g.number_of_nodes() > 0:
                    output_map_sv_path = (
                        f"{cfg.area.viz_path}/mapbox_{cfg.sv_name}_merged.html"
                    )
                    plot_graph(g, cfg.mapbox_token, output_map_sv_path, "blue", bucket)

                # Delete osm data from emory
                del sv_data

                return False
            else:
                return True
        else:
            return True

    # Initialize variables
    search_path = cfg.area.name
    continue_pipeline = True

    # Check for existence
    osm_merged_path = f"{cfg.area.data_path}/{cfg.osm_name}_merged.json"
    osm_merged_exists = bucket.blob(osm_merged_path).exists()
    if osm_merged_exists:
        logger.info(f"Broader area check -- OSM map merged already computed!")

    sv_merged_path = f"{cfg.area.data_path}/{cfg.sv_name}_merged.json"
    sv_merged_exists = bucket.blob(sv_merged_path).exists()
    if sv_merged_exists:
        logger.info(f"Broader area check -- SV map merged already computed!")

    if osm_merged_exists and sv_merged_exists:
        continue_pipeline = False

    # Run the search process
    while os.path.dirname(search_path) != "" and continue_pipeline:
        continue_pipeline = search(search_path)
        search_path = os.path.dirname(search_path)

    if continue_pipeline:
        logger.warning(
            f"Broader area check -- No broader regions found for faster computations"
        )
    else:
        logger.warning(f"Broader area check -- SV and OSM maps were already computed")

    return continue_pipeline


def check_iou(poly1: Polygon, poly2: Polygon, threshold: float = 0.95) -> bool:
    """
    Computes IOU between 2 shapes
    """
    # Calculate the intersection and union areas
    intersection_area = poly1.intersection(poly2).area
    union_area = poly1.union(poly2).area

    # Calculate the overlap ratio
    overlap_ratio = intersection_area / union_area

    # Check if the overlap ratio is over threshold
    return overlap_ratio >= threshold


def convert_nested_listconfig(obj):
    """Recursively convert ListConfig to Python lists."""
    if isinstance(obj, ListConfig):
        return [convert_nested_listconfig(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: convert_nested_listconfig(v) for k, v in obj.items()}
    return obj


def check_polygon(
    cfg: SetupConfig, bucket: Bucket
) -> Tuple[List[List[float]], Optional[gpd.GeoDataFrame]]:
    """
    Checks is the polygon is correctly defined and if a list of sub-polygons can be returned for
    a wider region than an area.
    Args:
        cfg: config dictionary
        bucket: GCS bucket

    Returns:
        a tuple containing the custom or predefined list of coordinates, and a GeoDataFrame of sub-polygons, if exists
    """

    # Split area, by region level
    level_names = cfg.area.name.split("/")
    level = len(level_names)
    polygons_base_path = f"{cfg.polygons_database_path}/{cfg.area.name}/polygons"

    # Read polygons database
    main_polygon, sub_polygons_df = read_region_polygons(
        cfg.area.name, polygons_base_path, bucket, level
    )

    if main_polygon is not None:
        # Get custom defined polygon
        if cfg.area.polygon is not None:
            custom_polygon_reversed = Polygon(
                [(lon, lat) for lat, lon in cfg.area.polygon]
            )
            overlap = check_iou(main_polygon, custom_polygon_reversed, threshold=0.99)

            # If the database shape and custom defined ones overlap, return the database one
            if overlap:
                logger.info(
                    f"Polygon check -- DB polygon found but defined polygon will be used for {cfg.area.name}"
                )
                final_polygon = reverse_lat_lon(main_polygon)
            else:
                # Raise an error if no custom defined or pre-defined polygons were found
                message = f"Polygon check -- DB polygon exists, but the defined one does not overlap with it!"
                logger.error(message)
                raise ValueError(message)
        else:
            logger.info(f"Polygon check -- DB polygon is used for {cfg.area.name}")
            final_polygon = reverse_lat_lon(main_polygon)
    else:
        last_area = cfg.area.name.split("/")[-1]
        if "_custom" in last_area:
            # Get custom polygon path
            polygon_path = (
                f"{cfg.database_path}/{cfg.area.name}/{cfg.custom_polygon_filename}"
            )

            if cfg.area.polygon is not None:

                # Check if it exists:
                if bucket.blob(polygon_path).exists():
                    logger.info(
                        f"Polygon check -- Custom-defined polygon EXISTS and is used for {cfg.area.name}"
                    )
                else:
                    # If not, save polygon to database
                    geojson_data = {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "Polygon",
                                    "coordinates": convert_nested_listconfig(
                                        cfg.area.polygon
                                    ),
                                },
                                "properties": {},  # Add metadata if needed
                            }
                        ],
                    }

                    # Upload polygon
                    upload_json_to_gcs(bucket, polygon_path, geojson_data)

                    logger.info(
                        f"Polygon check -- Custom-defined polygon WAS UPLOADED and is used for {cfg.area.name}"
                    )

                # Create polygon
                final_polygon = Polygon(cfg.area.polygon)
            else:
                # Only if it exists, read it
                if bucket.blob(polygon_path).exists():
                    polygon_geojson = read_json_gcs(bucket, source_file=polygon_path)
                    polygon = polygon_geojson["features"][0]["geometry"]["coordinates"]
                    final_polygon = Polygon(polygon)
                else:
                    message = (
                        f"Polygon check -- You need to define a custom-polygon, "
                        f"because none in DB and you used '_custom' prefix!"
                    )
                    logger.error(message)
                    raise ValueError(message)
        else:
            # Raise an error if no custom defined or pre-defined polygons were found
            message = f"Polygon check -- No polygon in the DB!"
            logger.error(message)
            raise ValueError(message)

    polygon_coords = list(final_polygon.exterior.coords)

    return polygon_coords, sub_polygons_df

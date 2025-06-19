import concurrent.futures
import json
import math
import os
import tempfile
from typing import List, Tuple

import networkx as nx
import networkx.readwrite.json_graph as json_graph
import numpy as np
from cloud_utils import read_json_gcs, upload_json_to_gcs
from config_model import SetupConfig
from general_utils import distance as dist
from google.cloud.storage import Bucket
from logger import logger
from numpy.typing import NDArray
from shapely import Polygon
from viz_utils import plot_graph


def compute_center(coordinates: List[Tuple[float, float]]) -> Tuple[float, float]:
    """
    Computes center of the coordinates given as parameter
    Args:
        coordinates: list of lat/lon coordinates defining a Polygon

    Returns:
        center of the Polygon in lat/lon coordinates
    """

    total_lat = sum(coord[0] for coord in coordinates)
    total_lon = sum(coord[1] for coord in coordinates)
    num_coords = len(coordinates)
    avg_lat = total_lat / num_coords
    avg_lon = total_lon / num_coords

    return avg_lat, avg_lon


def enlarge_polygon(
    coordinates: List[Tuple[float, float]], scaling_factor: float = 1.2
) -> List[Tuple[float, float]]:
    """
    Enlarges the polygon defined by coordinates, by the scaling factor
    Args:
        coordinates: list of lat/lon coordinates defining a Polygon
        scaling_factor:

    Returns:
        list of lat/lon coordinates of the enlarged polygon
    """
    # Calculate the center of the square
    center_lat, center_lon = compute_center(coordinates)

    # Determine the distance from the center to each corner of the square
    distances = []
    for coord in coordinates:
        delta_lat = center_lat - coord[0]
        delta_lon = center_lon - coord[1]
        distance = math.sqrt(delta_lat**2 + delta_lon**2)
        distances.append(distance)

    # Enlarge the distances by the scaling factor
    enlarged_distances = [distance * scaling_factor for distance in distances]

    # Calculate the new coordinates of the corners based on the enlarged distances
    new_coordinates = []
    for i, coord in enumerate(coordinates):
        delta_lat = center_lat - coord[0]
        delta_lon = center_lon - coord[1]
        angle = math.atan2(delta_lon, delta_lat)

        new_lat = center_lat + enlarged_distances[i] * math.cos(angle)
        new_lon = center_lon + enlarged_distances[i] * math.sin(angle)
        new_coordinates.append((new_lat, new_lon))

    return new_coordinates


def get_search_polygon(g: nx.Graph) -> Polygon:
    """Defines the Polygon where to search for new points, when applying the merging process"""

    min_lat, max_lat = 91, -91
    min_lon, max_lon = 181, -181

    # Get minimum and maximum latitude/longitude
    for node, data in g.nodes(data=True):
        if data["lat"] < min_lat:
            min_lat = data["lat"]
        if data["lat"] > max_lat:
            max_lat = data["lat"]
        if data["lon"] > max_lon:
            max_lon = data["lon"]
        if data["lon"] < min_lon:
            min_lon = data["lon"]

    # Build bottom left and top right corners
    bl_lat, bl_lon, tr_lat, tr_lon = min_lat, min_lon, max_lat, max_lon
    tl_lat, tl_lon = max_lat, min_lon
    br_lat, br_lon = min_lat, max_lon
    square_coords = [
        (bl_lat, bl_lon),
        (tl_lat, tl_lon),
        (tr_lat, tr_lon),
        (br_lat, br_lon),
    ]

    # Enlarge the polygon, by a defined scaling factor
    new_coords = enlarge_polygon(square_coords, scaling_factor=1.2)

    return Polygon(new_coords)


def merge_pair(g1, g2) -> nx.Graph:
    return nx.compose(g1, g2)


def parallel_merge(graphs: List[nx.Graph], max_workers: int) -> nx.Graph:
    """Create pairs of 2 graphs and merge them 2 by 2."""

    if not graphs:
        logger.info(f"[Empty graph list provided to parallel_merge.")
        return nx.Graph()

    while len(graphs) > 1:
        pairs = [(graphs[i], graphs[i + 1]) for i in range(0, len(graphs) - 1, 2)]
        if len(graphs) % 2 == 1:
            pairs.append((graphs[-1], nx.Graph()))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(merge_pair, g1, g2) for g1, g2 in pairs]
            graphs = [
                future.result() for future in concurrent.futures.as_completed(futures)
            ]
    return graphs[0]


def read_and_parse_json_gcs(bucket: Bucket, path: str) -> nx.Graph:
    """Reads JSON from GCS and parses it into a NetworkX graph."""
    json_data = read_json_gcs(bucket, path)
    return json_graph.adjacency_graph(json_data)


def save_intermediate_graph(graph: nx.Graph, directory: str, index: int) -> str:
    """Saves an intermediate graph to a temporary file and returns the file path."""
    path = os.path.join(directory, f"intermediate_graph_{index}.json")
    json_data = json_graph.adjacency_data(graph)
    with open(path, "w") as f:
        json.dump(json_data, f)
    return path


def load_intermediate_graph(path: str) -> nx.Graph:
    """Loads an intermediate graph from a file."""
    with open(path, "r") as f:
        json_data = json.load(f)
    return json_graph.adjacency_graph(json_data)


def merge_graphs_parallel(
    bucket: Bucket,
    window_graph_filenames: List[str],
    max_workers: int,
    type_name: str,
    batch_size: int = 100,
) -> nx.Graph:
    """
    Function for merging multiple graphs defined in the 'window_graph_filenames' list
    Args:
        bucket: GCS bucket
        window_graph_filenames: list of paths of graphs for one or more sub-windows
        max_workers: number of threads
        type_name: 'OSM' or 'SV'
        batch_size: Number of graphs to process in each batch

    Returns:
        merged networkx graph
    """

    logger.info(f"Merge -- computing {type_name} graphs")

    # Temporary directory for intermediate graphs
    with tempfile.TemporaryDirectory() as tmpdirname:
        intermediate_paths = []

        # Process in batches
        for i in range(0, len(window_graph_filenames), batch_size):
            batch_filenames = window_graph_filenames[i : i + batch_size]
            graphs = []

            # Use ThreadPoolExecutor for I/O bound operations
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                future_to_fname = {
                    executor.submit(read_and_parse_json_gcs, bucket, fname): fname
                    for fname in batch_filenames
                }
                for future in concurrent.futures.as_completed(future_to_fname):
                    fname = future_to_fname[future]
                    try:
                        graph = future.result()
                        graphs.append(graph)
                    except Exception as e:
                        logger.error(f"Error reading {fname}: {e}")

            # Merge graphs in the batch using threads
            batch_merged_graph = parallel_merge(graphs, max_workers)

            # Save the intermediate merged graph
            intermediate_path = save_intermediate_graph(
                batch_merged_graph, tmpdirname, len(intermediate_paths)
            )
            intermediate_paths.append(intermediate_path)
            logger.info(f"Merge -- Step {i}/{len(window_graph_filenames)} ")

        # Load and merge all intermediate graphs
        final_graphs = [load_intermediate_graph(path) for path in intermediate_paths]
        merged_graph = parallel_merge(final_graphs, max_workers)

    return merged_graph


def merge_sub_windows(
    cfg: SetupConfig, windows: NDArray[np.float64], bucket: Bucket
) -> None:
    """
    Merges multiple graphs from sub-windows with SV graphs, into one single graph
    Args:
        cfg: configuration object
        windows: np.ndarray[nr_sub_windows, 4]
        bucket: cloud bucket instance
    """

    # Retrieve merge configuration
    build_cfg = cfg.features.build

    merged_sv_path = f"{cfg.area.data_path}/{cfg.sv_name}_merged.json"
    merged_sv_map_exists = bucket.blob(merged_sv_path).exists()

    # If the SV merged map does not exist
    if (not merged_sv_map_exists) or cfg.force_compute_graph:

        # Get all available SV map filenames
        sub_window_sv_filenames = []
        for i, window in enumerate(windows):
            window_path = f"{cfg.area.output_path}/{cfg.sv_name}_{i}.json"
            sv_map_exists = bucket.blob(window_path).exists()

            if sv_map_exists:
                sub_window_sv_filenames.append(window_path)
            else:
                logger.info(f"Merge -- {window_path} was not created or has 0 points!")

        # Merge SV graph
        result_graph = merge_graphs_parallel(
            bucket, sub_window_sv_filenames, build_cfg.max_workers_merge, "SV"
        )

        # Compute edge length and store where needed
        for u, v in result_graph.edges():
            # Get the coordinates of the two nodes
            lat1, lon1 = result_graph.nodes[u]["lat"], result_graph.nodes[u]["lon"]
            lat2, lon2 = result_graph.nodes[v]["lat"], result_graph.nodes[v]["lon"]

            if not result_graph.edges[u, v].get("distance", None):
                # Calculate the Haversine distance
                distance = dist(lat1, lon1, lat2, lon2)
                # Add the distance as an edge attribute
                result_graph.edges[u, v]["distance"] = distance

        # Define output path
        out_path = f"{cfg.area.data_path}/{cfg.sv_name}_merged.json"

        # Export to json and upload to cloud
        json_data = json_graph.adjacency_data(result_graph)
        upload_json_to_gcs(bucket, out_path, json_data)

        # Visualize results
        if build_cfg.viz and result_graph.number_of_nodes() > 0:
            output_map_sv_path = f"{cfg.area.data_path}/{cfg.sv_name}_merged.html"
            plot_graph(
                result_graph, cfg.mapbox_token, output_map_sv_path, "blue", bucket
            )
    else:
        logger.info(f"Merged SV map already exists!")

    merged_osm_path = f"{cfg.area.data_path}/{cfg.osm_name}_merged.json"
    merged_osm_map_exists = bucket.blob(merged_osm_path).exists()

    # If the SV merged map does not exist
    if (not merged_osm_map_exists) or cfg.force_compute_graph:

        # Get all available OSM map filenames
        sub_window_osm_filenames = []
        for i, window in enumerate(windows):
            window_path = f"{cfg.area.output_path}/{cfg.osm_name}_{i}.json"

            osm_map_exists = bucket.blob(window_path).exists()
            if osm_map_exists:
                sub_window_osm_filenames.append(window_path)
            else:
                logger.info(f"Merge -- {window_path} was not created!")

        # Merge OSM graph
        result_graph = merge_graphs_parallel(
            bucket, sub_window_osm_filenames, build_cfg.max_workers_merge, "OSM"
        )
        # Compute edge length and store where needed
        for u, v in result_graph.edges():
            # Get the coordinates of the two nodes
            lat1, lon1 = result_graph.nodes[u]["lat"], result_graph.nodes[u]["lon"]
            lat2, lon2 = result_graph.nodes[v]["lat"], result_graph.nodes[v]["lon"]

            if not result_graph.edges[u, v].get("distance", None):
                # Calculate the Haversine distance
                distance = dist(lat1, lon1, lat2, lon2)
                # Add the distance as an edge attribute
                result_graph.edges[u, v]["distance"] = distance

        # Define output path
        out_path = f"{cfg.area.data_path}/{cfg.osm_name}_merged.json"

        # Export to json and upload to Cloud
        json_data = json_graph.adjacency_data(result_graph)
        upload_json_to_gcs(bucket, out_path, json_data)

        # Visualize results
        if build_cfg.viz and result_graph.number_of_nodes() > 0:
            output_map_osm_path = f"{cfg.area.data_path}/{cfg.osm_name}_merged.html"
            plot_graph(
                result_graph, cfg.mapbox_token, output_map_osm_path, "blue", bucket
            )
    else:
        logger.info(f"Merged OSM map already exists!")

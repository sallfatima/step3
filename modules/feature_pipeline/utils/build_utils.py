import ast
import os
import queue
import threading
import time
import urllib.parse as urlparse
from typing import Dict, List, Tuple

import networkx as nx
import networkx.readwrite.json_graph as json_graph
import numpy as np
import requests
import tqdm
from cloud_utils import get_signature, read_json_gcs, upload_json_to_gcs, upload_to_gcs
from config_model import SetupConfig
from finder_utils.run import driver_setup, find, replace_api_key
from general_utils import distance as dist
from google.cloud.storage import Bucket
from logger import logger
from logging_utils import format_logging
from networkx import parse_adjlist
from numpy.typing import NDArray
from osm_utils.utils.converter import convert_osm_to_roadgraph
from shapely import Point, Polygon
from viz_utils import plot_graph

# Global event for pausing and resuming threads
pause_event_find = threading.Event()
pause_event_find.set()

pause_event_osm = threading.Event()
pause_event_osm.set()


def set_overpass_headers() -> Dict[str, str]:
    """
    Sets headers for OverpassTurbo API
    Returns:
        Dictionary with header information
    """
    headers = {
        "Connection": "keep-alive",
        "sec-ch-ua": '"Google Chrome 80"',
        "Accept": "*/*",
        "Sec-Fetch-Dest": "empty",
        "User-Agent": "Automatic Pipeline",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://overpass-turbo.eu",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-Mode": "cors",
        "Referer": "https://overpass-turbo.eu/",
        "Accept-Language": "",
        "dnt": "1",
    }
    return headers


def retrieve_osm(
    coords: NDArray[np.float64],
    headers: Dict[str, str],
    base_url: str,
    cfg: SetupConfig,
) -> str:
    """
    Get XML formatted response from Overpass Turbo API, based on bounding box defined by coords
    Args:
        coords: (min_lon, max_lon, min_lat, max_lat)
        headers: dictionary with headers needed for the request
        base_url: base url for overpass turbo API
    Returns:
        String representation of OSM XML formatted response from Overpass Turbo API
    """

    # Get coordinates
    s, w, n, e = coords[2], coords[0], coords[3], coords[1]

    # Define query data
    # NOTE: Types of 'highway' are defined as in osm_utils/configuration.py -> 'car' type
    query = """
    [out:xml]/*fixed by auto repair*/[timeout:25];
    (
    way["highway"~"motorway|trunk|primary|secondary|tertiary|unclassified|residential|service|living_street|primary_link|secondary_link|tertiary_link|motorway_link"]({}, {}, {}, {});
    node(w);
    <;
    );
    out body;
    """.format(
        s, w, n, e
    )

    data = {"data": query}

    while True:
        pause_event_osm.wait()

        try:
            # Query OverPass to get XML formatted code based on the required coordinates in degrees
            response = requests.post(base_url, headers=headers, data=data)
            response.raise_for_status()

            return response.text
        except Exception as e:
            pause_event_osm.clear()
            logger.error(f"OSM file {cfg.area.name} -- Error at fetch: {e}")
            time.sleep(60)
            pause_event_osm.set()


def get_osm_run(
    base_url: str,
    bucket: Bucket,
    window_path: str,
    window_index: int,
    nr_windows: int,
    window: NDArray[np.float64],
    overpass_headers: Dict[str, str],
    cfg: SetupConfig,
) -> None:
    """Retrieves OSM data in XML formatted style"""

    # Check if present on cloud already
    exists = bucket.blob(window_path).exists()
    if not exists:
        # Retrieve OSM data, if not retrieved locally already
        response_text = retrieve_osm(window, overpass_headers, base_url, cfg)

        # Upload string output to GCS, under .osm file
        upload_to_gcs(bucket, response_text, window_path)

    # Log information
    log_text = format_logging(
        stage=f"OSM file {cfg.area.name}",
        progress=f"{window_index}/{nr_windows - 1}",
        exists=exists,
    )
    logger.info(log_text)


def get_osm(cfg: SetupConfig, windows: NDArray[np.float64], bucket: Bucket) -> None:
    """
    Saves OSM data for each sub-window in the windows array, remotely (and locally, depending on cfg)
    Args:
        cfg: configuration object
        windows: np.ndarray[nr_sub_windows, 4]
        bucket: cloud bucket instance
    """

    # Set OverpassTurbo API headers and url
    overpass_headers = set_overpass_headers()
    build_cfg = cfg.features.build

    base_url = build_cfg.overpass_url
    max_chunk_size = build_cfg.max_chunk_size_osm
    windows_indexes = list(range(len(windows)))
    for i in range(0, len(windows), max_chunk_size):
        cur_windows = windows_indexes[i : i + max_chunk_size]
        retrieve_threads = []

        for window_index in cur_windows:
            # Set sub-window path
            window_path = f"{cfg.area.output_path}/{cfg.osm_name}_{window_index}.osm"
            thread = threading.Thread(
                target=get_osm_run,
                args=(
                    base_url,
                    bucket,
                    window_path,
                    window_index,
                    len(windows),
                    windows[window_index],
                    overpass_headers,
                    cfg,
                ),
                daemon=True,
            )
            retrieve_threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in retrieve_threads:
            thread.join()


def remove_nodes_not_part_of_polygon(
    g: nx.Graph, polygon: Polygon, reverse_coords: bool = False
) -> nx.Graph:
    """
    Keeps points in area of interest
    Args:
        g: a networkx graph of OSM nodes
        polygon: shapely Polygon defining area of interest
        reverse_coords: true if coordinates of polygon are lat/lon, else lon/lat

    Returns:
        the new graph with points in area of interest.
    """

    # Remove nodes that are not part of the window
    nodes_to_remove = []
    for node, data in g.nodes(data=True):
        # Retrieve data
        if reverse_coords:
            c1, c2 = data["lon"], data["lat"]
        else:
            c1, c2 = data["lat"], data["lon"]

        point = Point(c1, c2)
        # Remove if not part of polygon
        if not polygon.contains(point):
            nodes_to_remove.append(node)

    # Remove nodes not part of desired area
    g.remove_nodes_from(nodes_to_remove)

    return g


def remove_nodes_not_part_of_window_or_polygon(
    g: nx.Graph, window_coords: NDArray[np.float64], polygon: Polygon
) -> nx.Graph:
    """
    Keeps points in area of interest
    Args:
        g: a netowrkx graph of OSM nodes
        window_coords: np.ndarray[1, 4]
        polygon: shapely Polygon defining area of interest

    Returns:
        The new graph with points in area of interest.
    """

    # Remove nodes that are not part of the window
    nodes_to_remove = []
    for node, data in g.nodes(data=True):
        # Retrieve data
        node_lat, node_lon = data["lat"], data["lon"]
        south, west, north, east = (
            window_coords[2],
            window_coords[0],
            window_coords[3],
            window_coords[1],
        )
        # Check if part of window
        if south <= node_lat <= north and west <= node_lon <= east:
            # Remove if not part of polygon
            point = Point(node_lat, node_lon)
            if not polygon.contains(point):
                nodes_to_remove.append(node)
        else:
            nodes_to_remove.append(node)

    # Remove nodes not part of desired area
    g.remove_nodes_from(nodes_to_remove)

    return g


def osm_to_graph_run(
    cfg: SetupConfig,
    bucket: Bucket,
    window_index: int,
    window: NDArray[np.float64],
    nr_windows: int,
) -> None:
    """Converts OSM data in XML format to OSM json graph"""

    # Retrieve config for build action
    build_cfg = cfg.features.build

    # Get paths
    json_graph_output_path = (
        f"{cfg.area.output_path}/{cfg.osm_name}_{window_index}.json"
    )
    osm_path = f"{cfg.area.output_path}/{cfg.osm_name}_{window_index}.osm"

    # Check if the road graph was not already created for an OSM file
    exists = bucket.blob(json_graph_output_path).exists()
    if not exists:

        # Create a json file with the roads network
        convert_osm_to_roadgraph(
            osm_path,
            build_cfg.network_type,
            build_cfg.unconnected_components,
            build_cfg.output_graph,
            build_cfg.contract_graph,
            build_cfg.enrich,
            build_cfg.distance_between_points,
            bucket=bucket,
        )
        # Read graph
        json_data = read_json_gcs(bucket, json_graph_output_path)
        g = json_graph.adjacency_graph(json_data)

        # Transform to undirected
        g = g.to_undirected()

        # Remove nodes that are not part of the window or polygon
        polygon = Polygon(cfg.area.polygon)
        g = remove_nodes_not_part_of_window_or_polygon(g, window, polygon)

        # Relabel nodes IDs
        node_mapping = {
            node: f"{data['lon']},{data['lat']}" for node, data in g.nodes(data=True)
        }
        g = nx.relabel_nodes(g, node_mapping)

        # Compute edge length and store on edge
        for u, v in g.edges():
            # Get the coordinates of the two nodes
            lat1, lon1 = g.nodes[u]["lat"], g.nodes[u]["lon"]
            lat2, lon2 = g.nodes[v]["lat"], g.nodes[v]["lon"]

            # Calculate the Haversine distance
            distance = dist(lat1, lon1, lat2, lon2)

            # Add the distance as an edge attribute
            g.edges[u, v]["distance"] = distance

        # Build the dict data from the graph and write to cloud
        graph_dict = json_graph.adjacency_data(g)
        upload_json_to_gcs(bucket, json_graph_output_path, graph_dict)

        if build_cfg.viz and g.number_of_nodes() > 0:
            output_map_path = f"{cfg.area.viz_path}/{cfg.osm_name}_{window_index}.html"
            plot_graph(g, cfg.mapbox_token, output_map_path, "red", bucket)

    # Log information
    log_text = format_logging(
        stage="OSM to JSON graph",
        progress=f"{window_index}/{nr_windows - 1}",
        exists=exists,
    )
    logger.info(log_text)


def osm_to_graph(
    cfg: SetupConfig, windows: NDArray[np.float64], bucket: Bucket
) -> None:
    """
    Converts OSM data to a JSON graph of points
    Args:
        cfg: configuration object
        windows: np.ndarray[nr_sub_windows, 4]
        bucket: cloud bucket instance
    """

    max_chunk_size = cfg.features.build.max_chunk_size_osm_to_graph
    windows_indexes = list(range(len(windows)))
    for i in range(0, len(windows), max_chunk_size):
        cur_windows = windows_indexes[i : i + max_chunk_size]
        retrieve_threads = []

        for window_index in cur_windows:
            thread = threading.Thread(
                target=osm_to_graph_run,
                args=(cfg, bucket, window_index, windows[window_index], len(windows)),
                daemon=True,
            )
            retrieve_threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in retrieve_threads:
            thread.join()


def build_mapping_adjacency(
    image_res: List[List[str | List[dict]]],
    lats: Tuple[float, ...],
    lons: Tuple[float, ...],
) -> Tuple[Dict[str, Tuple[Tuple[float, float], str, str]], List[str]]:
    """Builds mapping from OSM points to available SV locations"""

    # Initialize dict and adjacency list
    nodes_mapping_dict = {}
    adjacency_list = []

    # Build mapping from current node to found result/location
    for j, res in enumerate(image_res):
        if res not in ["NO_RESULTS", "SAME"]:
            # Retrieve location, date and panoID data
            location = ast.literal_eval(res[0])
            date = res[2]
            pano = res[1]
            nodes_mapping_dict[f"{lats[j]},{lons[j]}"] = (location, pano, date)

            # Add row to adjacency list
            links = res[3]
            targets_pano_str = ""
            for link in links:
                targets_pano_str += f"{link['pano']} "
            adjacency_list.append(f"{pano} {targets_pano_str[:-1]}")

    # Remove results where retrieved location is the same
    seen_values = set()
    result_dict = {}
    for key, value in nodes_mapping_dict.items():
        if value[0] not in seen_values:
            result_dict[key] = value
            seen_values.add(value[0])

    return result_dict, adjacency_list


def retrieve_location_run(
    api_key: str, secret: str, pano: str, result_queue: queue.Queue
) -> None:
    """Retrieves lat/lon location for a specific pano ID"""

    # default urls
    meta_base = "https://maps.googleapis.com/maps/api/streetview/metadata?"

    # define the params for the metadata request
    meta_params = {"key": api_key, "pano": pano}

    input_url_meta = meta_base + urlparse.urlencode(meta_params)
    meta_params["signature"] = get_signature(input_url_meta, secret)

    while True:
        # Wait if the pause event is set
        pause_event_find.wait()

        try:
            meta_response = requests.get(meta_base, params=meta_params, verify=False)
            meta_response.raise_for_status()  # Raise an HTTPError for bad responses

            meta_data = meta_response.json()
            if meta_data["status"] == "OK":
                lat, lon = meta_data["location"]["lat"], meta_data["location"]["lng"]
                date = meta_data["date"]
                result_queue.put((pano, (lat, lon), date))
            else:
                # ZERO_RESULT can be present here
                result_queue.put((pano, None, None))

            # Exit the loop if request is successful
            break

        except Exception as e:
            logger.error(f"Location retrieve -- Error at fetch: {e}")
            pause_event_find.clear()
            time.sleep(60)
            pause_event_find.set()


def retrieve_location_date(
    cfg: SetupConfig,
    graph: nx.Graph,
    nodes_mapping_dict: Dict[str, Tuple[Tuple[float, float], str, str]],
) -> nx.Graph:
    """Adds lat lon as attributes to a graph with nodes as pano IDs"""

    # Get config parameters
    max_chunk_size = cfg.features.build.max_chunk_retrieve_location
    api_key = cfg.google_token
    secret = cfg.google_secret

    # Build set of nodes in SV graph
    nodes_set = set(graph.nodes())

    # Build set of nodes that have the location already
    location_available_set = set()
    for (location, pano, date) in nodes_mapping_dict.values():
        if pano in nodes_set:
            graph.nodes[pano]["lat"] = location[0]
            graph.nodes[pano]["lon"] = location[1]
            graph.nodes[pano]["date"] = date
            location_available_set.add(pano)

    # Get necessary set of nodes for finding the location
    nodes = list(nodes_set.difference(location_available_set))

    # Initialize queue
    result_queue = queue.Queue()

    # Process chunks of nodes
    for i in range(0, len(nodes), max_chunk_size):
        cur_nodes = nodes[i : i + max_chunk_size]
        retrieve_threads = []  # Initialize the list for each chunk

        for node in cur_nodes:
            if graph.nodes[node].get("lat"):
                continue

            # Start threads equal to the number of nodes in a chunk
            thread = threading.Thread(
                target=retrieve_location_run,
                args=(api_key, secret, node, result_queue),
                daemon=True,
            )
            retrieve_threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in retrieve_threads:
            thread.join()

    # Collect results from the queue
    while not result_queue.empty():
        pano, location, date = result_queue.get()
        if location is not None:
            lat, lon = location
            graph.nodes[pano]["lat"] = lat
            graph.nodes[pano]["lon"] = lon
            graph.nodes[pano]["date"] = date

    return graph


def remove_big_edges(g: nx.Graph, thresh: int = None, q: int = 90) -> nx.Graph:
    """
    Removes big edges according to the thresh (meters) parameter or to q (quantile)
    Args:
        g: graph
        thresh: maximum length in meters
        q: quantile

    Returns:
        new graph with big edges removed
    """

    if thresh is None:
        assert q in list(range(45, 100, 5)), "Bad value for quantile"
        distances = []
        # Compute quantiles based on edge lengths:
        for u, v, data in g.edges(data=True):
            distances.append(
                dist(
                    g.nodes[u]["lat"],
                    g.nodes[u]["lon"],
                    g.nodes[v]["lat"],
                    g.nodes[v]["lon"],
                )
            )
        thresh = np.percentile(np.array(distances), [q])

    edges_to_remove = [
        (u, v)
        for u, v, data in g.edges(data=True)
        if dist(
            g.nodes[u]["lat"], g.nodes[u]["lon"], g.nodes[v]["lat"], g.nodes[v]["lon"]
        )
        > thresh
    ]
    g.remove_edges_from(edges_to_remove)

    return g


def get_available_sv_run(
    cfg: SetupConfig,
    bucket: Bucket,
    html_file_path: str,
    index: int,
    nr_windows: int,
    stop_event: threading.Event,
) -> None:
    """Runs the SV location finding for one sub-window"""

    if stop_event.is_set():
        return

    build_cfg = cfg.features.build
    sv_graph_path = f"{cfg.area.output_path}/{cfg.sv_name}_{index}.json"
    sv_graph_exists = bucket.blob(sv_graph_path).exists()

    # If the mapping are not computed
    if not sv_graph_exists:

        osm_json_path = f"{cfg.area.output_path}/{cfg.osm_name}_{index}.json"
        osm_json_exists = bucket.blob(osm_json_path).exists()

        # If the OSM json file was created
        if osm_json_exists:
            # Read roads graph
            json_data = read_json_gcs(bucket, osm_json_path)
            g = json_graph.adjacency_graph(json_data)

            # The graph can have 0 points, avoid that
            if not g.number_of_nodes() > 0:
                logger.info(
                    f"SV file {cfg.area.name} -- no OSM points for sub-window {index}/{nr_windows - 1}"
                )
                return

            image_res = []
            lats = ()
            lons = ()

            attempt = 0
            max_attempts = 10
            seconds_timeout = 120
            driver = None

            while attempt < max_attempts:
                try:
                    # Initialize driver
                    driver = driver_setup()

                    # Find SV locations
                    image_res, lats, lons = find(
                        g,
                        html_file_path,
                        driver,
                        radius=build_cfg.distance_between_points * 2,
                        cfg=cfg
                    )
                    break

                except Exception as e:
                    attempt += 1
                    logger.error(
                        f"SV file {cfg.area.name} -- ERROR encountered in thread: {e}"
                    )
                    if attempt >= max_attempts:
                        stop_event.set()
                    else:
                        logger.warning(f"Retrying in {seconds_timeout} seconds...")
                        time.sleep(seconds_timeout)
                finally:
                    if driver:
                        # Ensure driver is always quit
                        driver.quit()

            # Build mapping from current nodes to found locations and build SV adjacency list
            all_negative = all(
                element in ["NO_RESULTS", "SAME"] for element in image_res
            )

            if not all_negative:
                nodes_mapping_dict, pano_adjacency_list_str = build_mapping_adjacency(
                    image_res, lats, lons
                )

                # Compute graph based on adjacency list
                pano_graph = parse_adjlist(pano_adjacency_list_str, nodetype=str)

                # Retrieve location for nodes that do not have location information
                pano_graph = retrieve_location_date(cfg, pano_graph, nodes_mapping_dict)

                # Remove big edges
                pano_graph = remove_big_edges(
                    pano_graph, thresh=build_cfg.big_edges_thresh
                )

                # compute edge length and store it
                for u, v in pano_graph.edges():
                    # Get the coordinates of the two nodes
                    lat1, lon1 = pano_graph.nodes[u]["lat"], pano_graph.nodes[u]["lon"]
                    lat2, lon2 = pano_graph.nodes[v]["lat"], pano_graph.nodes[v]["lon"]

                    # Calculate the Haversine distance
                    distance = dist(lat1, lon1, lat2, lon2)

                    # Add the distance as an edge attribute
                    pano_graph.edges[u, v]["distance"] = distance

                # Write graph to file
                graph_dict = json_graph.adjacency_data(pano_graph)
                upload_json_to_gcs(bucket, sv_graph_path, graph_dict)

                if build_cfg.viz and pano_graph.number_of_nodes() > 0:
                    output_map_graph_path = (
                        f"{cfg.area.viz_path}/{cfg.sv_name}_{index}.html"
                    )
                    plot_graph(
                        pano_graph,
                        cfg.mapbox_token,
                        output_map_graph_path,
                        "blue",
                        bucket,
                    )

                logger.info(
                    f"SV file {cfg.area.name} -- retrieved for sub-window {index}/{nr_windows - 1}"
                )
            else:
                logger.info(
                    f"SV file {cfg.area.name} -- no SV points for sub-window {index}/{nr_windows - 1}"
                )
        else:
            logger.info(
                f"SV file {cfg.area.name} -- no OSM file for sub-window {index}/{nr_windows - 1}"
            )
    else:
        logger.info(
            f"SV file {cfg.area.name} -- already computed for sub-window {index}/{nr_windows - 1}"
        )


def get_available_sv(
    cfg: SetupConfig, windows: NDArray[np.float64], bucket: Bucket
) -> None:
    """
    Finds available street view locations, corresponding to OSM points
    Args:
        cfg: config dictionary
        windows: sub-window polygons that form the desired area
        bucket: GCS bucket
    """

    # Define stop event
    stop_event = threading.Event()

    # Replace the Google token in the predefined HTML file
    html_file_path = replace_api_key(cfg.google_token)

    # Start threads equal to the number of nodes in a chunk
    max_chunk_size = cfg.features.build.max_chunk_size_find

    # Resume from iteration
    resume_from = cfg.features.build.resume_sv_find_from

    windows_indexes = list(range(len(windows)))
    for i in range(resume_from, len(windows), max_chunk_size):
        cur_windows = windows_indexes[i : i + max_chunk_size]
        retrieve_threads = []

        for window_index in cur_windows:
            thread = threading.Thread(
                target=get_available_sv_run,
                args=(
                    cfg,
                    bucket,
                    html_file_path,
                    window_index,
                    len(windows),
                    stop_event,
                ),
                daemon=True,
            )
            retrieve_threads.append(thread)
            thread.start()
            time.sleep(1)  # Delay between thread starts

        for thread in retrieve_threads:
            thread.join()

        # After joining all threads, check if stop_event is set, indicating failure
        if stop_event.is_set():
            logger.error("Program terminated due to repeated errors in threads.")
            raise RuntimeError("Program terminated due to repeated errors in threads.")

    # Remove temporary html file
    os.remove(html_file_path)

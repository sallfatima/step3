import io
import math
import mimetypes
import threading
import time
import urllib.parse as urlparse
from itertools import combinations
from typing import Dict, List, Optional, Tuple

import networkx as nx
import networkx.readwrite.json_graph as json_graph
import numpy as np
import requests
from cloud_utils import get_bucket, get_signature, read_json_gcs
from config_model import SetupConfig
from google.cloud.storage import Bucket
from logger import logger
from logging_utils import format_logging
from numpy.typing import NDArray

# Global event to handle pausing threads
pause_event = threading.Event()
pause_event.set()


def is_within_ranges(first_angle: float, second_angle: float, thresh: int = 45) -> bool:

    # Adjust angles to be within [0, 360) range
    first_angle = first_angle % 360
    first_angle_opposite = (first_angle + 180) % 360
    second_angle = second_angle % 360

    # Define the specified ranges
    ranges = [
        ((first_angle - thresh) % 360, (first_angle + thresh) % 360),
        ((first_angle_opposite - thresh) % 360, (first_angle_opposite + thresh) % 360),
    ]

    # Check if the second angle is within any of the specified ranges
    for range_start, range_end in ranges:
        if range_start > range_end:
            if (range_start <= second_angle <= 360) or (0 <= second_angle <= range_end):
                return True
        if range_start <= second_angle <= range_end:
            return True

    return False


def check_edge_headings(
    edges_headings: List[Tuple[float, float, float]], thresh: int = 45
) -> NDArray[np.float64]:
    good_headings_indexes = []
    all_comb = list(combinations(list(range(len(edges_headings))), 2))
    for comb in all_comb:
        # Get headings
        heading1 = edges_headings[comb[0]][0]
        heading2 = edges_headings[comb[1]][0]

        # Order headings
        if heading1 > heading2:
            bigger_heading = heading1
            smaller_heading = heading2
        else:
            bigger_heading = heading2
            smaller_heading = heading1
        if is_within_ranges(smaller_heading, bigger_heading, thresh):
            # Choose only one of the heading index
            good_headings_indexes.append(comb[0])
        else:
            # Take both heading indexes
            good_headings_indexes.extend([comb[0], comb[1]])
    good_headings_indexes = list(set(good_headings_indexes))

    return np.array(edges_headings)[good_headings_indexes]


def calculate_heading(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> Tuple[float, float, float]:
    # Convert latitude and longitude from degrees to radians
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = map(math.radians, [lat1, lon1, lat2, lon2])

    # Calculate differences in coordinates
    d_lon = lon2_rad - lon1_rad

    # Calculate heading using trigonometry
    y = math.sin(d_lon) * math.cos(lat2_rad)
    x = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(
        lat2_rad
    ) * math.cos(d_lon)
    heading_rad = math.atan2(y, x)

    # Convert heading from radians to degrees and ensure it's in the range [0, 360)
    heading = (math.degrees(heading_rad) + 360) % 360

    clockwise_heading = (heading + 90) % 360
    counterclockwise_heading = (heading - 90) % 360

    return heading, clockwise_heading, counterclockwise_heading


def get_street_image(
    cfg: SetupConfig,
    lat: float,
    lon: float,
    params: Dict[str, List[int] | int | float],
    side_index: int,
    bucket: Bucket,
    public_bucket: Bucket,
    heading_index: int,
) -> None:
    """Obtain a Google Street View image and store it to cloud"""

    while True:
        pause_event.wait()
        try:
            # default urls
            meta_base = "https://maps.googleapis.com/maps/api/streetview/metadata?"
            pic_base = "https://maps.googleapis.com/maps/api/streetview?"

            # define the params for the metadata request
            meta_params = {"key": cfg.google_token, "location": f"{lat}, {lon}"}

            # define the params for the picture request
            size = params["size"]
            heading = params["heading"]
            fov = params["fov"]
            pitch = params["pitch"]

            pic_params = {
                "key": cfg.google_token,
                "location": f"{lat}, {lon}",
                "size": f"{size[0]}x{size[1]}",
                "fov": fov,
                "pitch": pitch,
            }

            if heading is not None:
                pic_params["heading"] = heading

            input_url_meta = meta_base + urlparse.urlencode(meta_params)
            meta_params["signature"] = get_signature(input_url_meta, cfg.google_secret)

            input_url_pic = pic_base + urlparse.urlencode(pic_params)
            pic_params["signature"] = get_signature(input_url_pic, cfg.google_secret)

            # obtain the metadata of the request (free)
            meta_response = requests.get(meta_base, params=meta_params)

            # display the contents of the response
            meta_data = meta_response.json()
            if meta_data["status"] == "OK":
                date = meta_data["date"]

                img_name = f"{lat}_{lon}_{heading_index}_{side_index}_{heading}_{fov}_{date}.jpg"

                # Check if present on cloud already
                image_path = f"{cfg.area.images_path}/{img_name}"
                image_blob = bucket.blob(image_path)
                image_exists = image_blob.exists()

                if not image_exists:
                    # Proceed to call for a new image
                    pic_response = requests.get(pic_base, params=pic_params)

                    if pic_response.status_code == 200:
                        image_stream = io.BytesIO(pic_response.content)

                        content_type, _ = mimetypes.guess_type(image_path)
                        image_blob.upload_from_file(
                            image_stream, content_type=content_type, rewind=True
                        )

                        # Also write to public bucket
                        if public_bucket is not None:
                            image_blob_public = public_bucket.blob(image_path)
                            image_blob_public.upload_from_file(
                                image_stream, content_type=content_type, rewind=True
                            )
                    else:
                        logger.error(
                            f"Retrieve {cfg.area.name} -- Error at retrieving image {img_name}"
                        )
                        raise Exception(
                            f"Retrieve {cfg.area.name} -- Error at retrieving image {img_name}"
                        )
                    # close the response connection to the API
                    pic_response.close()

                text = format_logging(
                    img_name,
                    stage="Retrieve",
                    exists=image_exists,
                    area_name=cfg.area.name,
                )
                logger.info(text)
                break
            else:
                logger.error(f"Retrieve {cfg.area.name} -- Error at metadata")
                raise Exception("Metadata retrieval failed")

        except Exception as e:
            pause_event.clear()  # Clear flag
            logger.error(f"Error in get_street_image: {e}.")
            time.sleep(60)
            pause_event.set()  # Set the pause event to pause all threads


def retrieve_run(
    node: str,
    g: nx.Graph,
    img_params: Dict[str, List[int] | int | float],
    cfg: SetupConfig,
    bucket: Bucket,
    public_bucket: Optional[Bucket],
) -> None:
    """Retrieves associated images for a node"""

    # Get latitude, longitude data
    data = g.nodes[node]
    node_lat, node_lon = data["lat"], data["lon"]

    # Get neighbours
    neighbours = list(g.neighbors(node))

    # Get node degree
    node_degree = g.degree(node)
    if node_degree == 1:
        # Get neighbour coordinates
        neigh = neighbours[0]
        neigh_lat, neigh_lon = g.nodes[neigh]["lat"], g.nodes[neigh]["lon"]

        # Compute heading
        edge_heading, h1, h2 = calculate_heading(
            node_lat, node_lon, neigh_lat, neigh_lon
        )
        for side_index, h in enumerate([h1, h2]):
            img_params["heading"] = h

            # Retrieve image
            get_street_image(
                cfg,
                data["lat"],
                data["lon"],
                img_params,
                side_index,
                bucket,
                public_bucket,
                heading_index=0,
            )

    elif node_degree >= 2:
        neigh_edges_headings = []
        for neigh in neighbours:
            # Get neighbour coordinates
            neigh_lat, neigh_lon = g.nodes[neigh]["lat"], g.nodes[neigh]["lon"]

            # Compute heading
            edge_heading, h1, h2 = calculate_heading(
                node_lat, node_lon, neigh_lat, neigh_lon
            )
            neigh_edges_headings.append((edge_heading, h1, h2))

        # Keep useful headings
        headings_needed = check_edge_headings(neigh_edges_headings)

        for heading_index, (_, h1, h2) in enumerate(headings_needed):
            for side_index, h in enumerate([h1, h2]):
                img_params["heading"] = h

                # Retrieve image
                get_street_image(
                    cfg,
                    data["lat"],
                    data["lon"],
                    img_params,
                    side_index,
                    bucket,
                    public_bucket,
                    heading_index=heading_index,
                )
    else:
        # If it happens that there is still a node with degree 0, skip it, as we cannot compute the heading
        return


def retrieve_images(cfg: SetupConfig, bucket: Bucket) -> None:
    """Retrieves Street View images"""

    # Get retrieve action config
    retrieve_cfg = cfg.features.retrieve
    max_chunk_size = retrieve_cfg.max_chunk_size

    public_bucket = None
    if retrieve_cfg.public:
        public_bucket = get_bucket(cfg.public_bucket_name, cfg.project_id)

    # Get graph of SV data
    sv_graph_path = f"{cfg.area.data_path}/{cfg.sv_name}_merged.json"
    sv_merged_exists = bucket.blob(sv_graph_path).exists()
    assert sv_merged_exists, logger.error(
        f"Retrieve {cfg.area.name} -- SV map not computed. "
        f"Please run build and merge processes before!"
    )

    # Set image params
    img_params = {
        "size": retrieve_cfg.img_size,
        "fov": retrieve_cfg.fov,
        "pitch": retrieve_cfg.pitch,
    }

    # Read graph with all points
    json_sv = read_json_gcs(bucket, sv_graph_path)
    g = json_graph.adjacency_graph(json_sv)

    # Get disconnected components
    components = list(nx.connected_components(g))

    for comp_index, component in enumerate(components):
        logger.info(f"Processing component {comp_index + 1}/{len(components)}....")
        bfs_ordered_nodes = list(
            nx.bfs_tree(g.subgraph(component), source=list(component)[0]).nodes()
        )

        # Process chunks of nodes
        for i in range(0, len(bfs_ordered_nodes), max_chunk_size):
            cur_nodes = bfs_ordered_nodes[i : i + max_chunk_size]
            retrieve_threads = []

            # Start threads equal to the number of nodes in a chunk
            for node in cur_nodes:
                new_image_params = img_params.copy()
                thread = threading.Thread(
                    target=retrieve_run,
                    args=(node, g, new_image_params, cfg, bucket, public_bucket),
                    daemon=True,
                )
                retrieve_threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in retrieve_threads:
                thread.join()

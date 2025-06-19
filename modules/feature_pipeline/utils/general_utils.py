import json
import math
from math import acos, cos, pi, sin
from typing import List

import networkx as nx
import networkx.readwrite.json_graph as json_graph
import numpy as np
import yaml
from numpy.typing import NDArray
from shapely import Polygon


# from http://www.johndcook.com/blog/python_longitude_latitude/
def distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Computes the distance between 2 points defined by lat/lon
    Args:
        lat1: Latitude of first points
        lon1: Longitude of first point
        lat2: Latitude of second points
        lon2: Longitude of second point

    Returns:
        distance in meters between the 2 points
    """

    # Convert latitude and longitude to
    # spherical coordinates in radians.
    degrees_to_radians = pi / 180.0

    # phi = 90 - latitude
    phi1 = (90.0 - lat1) * degrees_to_radians
    phi2 = (90.0 - lat2) * degrees_to_radians

    # theta = longitude
    theta1 = lon1 * degrees_to_radians
    theta2 = lon2 * degrees_to_radians

    cos_val = sin(phi1) * sin(phi2) * cos(theta1 - theta2) + cos(phi1) * cos(phi2)
    cos_val = min(1, cos_val)
    arc_val = acos(cos_val)

    return arc_val * 6373000


def read_graph_json(json_path: str) -> nx.DiGraph:
    with open(json_path, "r", encoding="utf-8") as f:
        js_graph = json.load(f)
        g = json_graph.adjacency_graph(js_graph)
    return g


def write_graph_json(g: nx.Graph, json_path: str) -> None:
    data = json_graph.adjacency_data(g)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def read_yaml(filepath: str) -> dict:
    with open(filepath) as stream:
        data_loaded = yaml.safe_load(stream)

    return data_loaded


def reverse_lat_lon(polygon: Polygon) -> Polygon:
    """Reverse the lat/lon coordinates in a shapely Polygon"""

    # Extract the exterior coordinates of the polygon
    exterior_coords = polygon.exterior.coords

    # Reverse the coordinates (swap lat and lon)
    reversed_coords = [(lon, lat) for lat, lon in exterior_coords]

    # Create a new polygon with the reversed coordinates
    reversed_polygon = Polygon(reversed_coords)

    return reversed_polygon


def enclosing_rectangle(coordinates: List[List[float]]) -> List[float]:
    """
    Find enclosing rectangle for a list of lat/lon coordinates defining a Polygon
    Args:
        coordinates: list of lat/lon coordinates

    Returns:
        bottom left lat/lon and upper right lat/lon of the enclosing rectangle
    """
    min_lat = min(coord[0] for coord in coordinates)
    min_lon = min(coord[1] for coord in coordinates)
    max_lat = max(coord[0] for coord in coordinates)
    max_lon = max(coord[1] for coord in coordinates)

    return [min_lat, min_lon, max_lat, max_lon]


def order_windows_coordinates(windows: NDArray[np.float64]) -> NDArray[np.float64]:
    """
    Reorder lat/lon points for each window in windows array
    Args:
        windows: np.ndarray[num_sub_windows, 4]

    Returns:
        np.ndarray[num_sub_windows, 4] -> the order on last axis is: (min_lon, max_lon, min_lat, max_lat)
    """
    for i, w in enumerate(windows):
        if w[0] > w[1]:
            windows[i, [0, 1]] = windows[i, [1, 0]]
        if w[2] > w[3]:
            windows[i, [2, 3]] = windows[i, [3, 2]]
    return windows


def split_window(
    orig_window: List[float], sub_window_area: List[float], window_overlap: int = 20
) -> NDArray[np.float64]:
    """
    Split orig_window into multiple smaller sub_window_area
    Args:
        orig_window: array of original window coordinates - bottom left and upper right lat/lon coordinates
        sub_window_area: [w, h] in meters
        window_overlap: meters of sub_window overlap

    Returns:
        np.ndarray[num_sub_windows, 4] -> the order on last axis is: (min_lon, max_lon, min_lat, max_lat)
    """

    lat1, lon1, lat2, lon2 = orig_window
    assert lat1 < lat2 and lon1 < lon2, "Window coordinates ERROR"

    if distance(lat1, lon1, lat2, lon1) < sub_window_area[0]:
        sub_window_area[0] = distance(lat1, lon1, lat2, lon1)
        lat_steps = 1
        overlap_w = 0
    else:
        overlap_w = window_overlap
        lat_steps = (
            int(distance(lat1, lon1, lat2, lon1) / (sub_window_area[0] - overlap_w)) + 1
        )
    if distance(lat1, lon1, lat1, lon2) < sub_window_area[1]:
        sub_window_area[1] = distance(lat1, lon1, lat1, lon2)
        lon_steps = 1
        overlap_h = 0
    else:
        overlap_h = window_overlap
        lon_steps = (
            int(distance(lat1, lon1, lat1, lon2) / (sub_window_area[1] - overlap_h)) + 1
        )

    # Iterate through latitude steps
    result = []
    for i in range(lat_steps):
        lat_start = lat1 + (i * (sub_window_area[0] - overlap_w)) / (
            2 * math.pi * 6371000 / 360
        )
        lat_end = lat_start + (sub_window_area[0] / (2 * math.pi * 6371000 / 360))

        # Iterate through longitude steps
        for j in range(lon_steps):
            lon_start = lon1 + (j * (sub_window_area[1] - overlap_h)) / (
                2 * math.pi * 6371000 / 360
            )
            lon_end = lon_start + (sub_window_area[1] / (2 * math.pi * 6371000 / 360))

            # Append coordinates of the current smaller area to the result list
            result.append([lon_start, lon_end, lat_start, lat_end])

    # Apply this just to be sure the coordinates order is good
    ordered_windows = order_windows_coordinates(np.array(result))

    return ordered_windows

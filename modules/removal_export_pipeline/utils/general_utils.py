import os
from typing import List, Tuple

from shapely import Polygon


def replace_in_positions(
    string: str, target_char: str, replacement_char: str, positions: Tuple
) -> str:
    count = 0
    new_string = ""
    for char in string:
        if char == target_char:
            count += 1
            if count in list(positions):
                new_string += replacement_char
            else:
                new_string += char
        else:
            new_string += char
    return new_string


def filepath_from_roboflow(
    img_path: str, chr_to_replace: str = "_", positions: Tuple[int] = (1, 3, 7)
) -> str:
    """Gets an image name that was stored using Roboflow's format"""

    # Get only image name in ROboflow format
    img_name_cloud = os.path.basename(img_path).split("_jpg")[0]

    # Replace '_' with '.' in lat, lon, and heading
    img_name_cloud = replace_in_positions(
        img_name_cloud, chr_to_replace, ".", positions
    )

    # Set new image path
    img_name_cloud = f"{os.path.dirname(img_path)}/{img_name_cloud}.jpg"

    return img_name_cloud


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


def reverse_lat_lon(polygon: Polygon) -> Polygon:
    """Reverse the lat/lon coordinates in a shapely Polygon"""

    # Extract the exterior coordinates of the polygon
    exterior_coords = polygon.exterior.coords

    # Reverse the coordinates (swap lat and lon)
    reversed_coords = [(lon, lat) for lat, lon in exterior_coords]

    # Create a new polygon with the reversed coordinates
    reversed_polygon = Polygon(reversed_coords)

    return reversed_polygon

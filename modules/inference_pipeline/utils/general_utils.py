from shapely import Polygon


def reverse_lat_lon(polygon: Polygon) -> Polygon:
    """Reverse the lat/lon coordinates in a shapely Polygon"""

    # Extract the exterior coordinates of the polygon
    exterior_coords = polygon.exterior.coords

    # Reverse the coordinates (swap lat and lon)
    reversed_coords = [(lon, lat) for lat, lon in exterior_coords]

    # Create a new polygon with the reversed coordinates
    reversed_polygon = Polygon(reversed_coords)

    return reversed_polygon

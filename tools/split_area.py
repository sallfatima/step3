from typing import List

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
from shapely.geometry import Polygon, box


def split_area(coords: List[List[float]], n_splits: int, area_name: str):
    """
    Splits area defined by coordinates as a List of [lat,lon] pairs into n_splits.
    Args:
        coords: list of (lat, lon) lists defining a polygon
        n_splits: number of splits
        area_name: name of area

    Returns:

    """
    polygon = Polygon(coords)

    # Compute the bounding box and grid dimensions
    minx, miny, maxx, maxy = polygon.bounds
    x_splits = int(np.sqrt(n_splits))  # Number of splits along x-axis
    y_splits = int(np.ceil(n_splits / x_splits))  # Number of splits along y-axis

    # Create a grid to split the polygon
    x_step = (maxx - minx) / x_splits
    y_step = (maxy - miny) / y_splits
    grid = []

    for i in range(x_splits):
        for j in range(y_splits):
            x_start = minx + i * x_step
            x_end = x_start + x_step
            y_start = miny + j * y_step
            y_end = y_start + y_step
            grid.append(box(x_start, y_start, x_end, y_end))

    # Intersect the grid with the original polygon
    subpolygons = [
        polygon.intersection(cell) for cell in grid if polygon.intersects(cell)
    ]

    # Filter out empty geometries
    subpolygons = [p for p in subpolygons if not p.is_empty]

    # Create a GeoDataFrame for visualization or further processing
    gdf = gpd.GeoDataFrame({"geometry": subpolygons})  # .to_crs("EPSG:4326")

    # Save and visualize
    gdf.to_file(f"{area_name}.geojson", driver="GeoJSON")

    fig, ax = plt.subplots(figsize=(8, 8))  # Adjust the figure size as needed
    gdf.plot(ax=ax, color="lightblue", edgecolor="black")

    # Add polygon indices at centroids
    for idx, row in gdf.iterrows():
        centroid = row.geometry.centroid
        ax.text(
            centroid.x,
            centroid.y,
            str(idx + 1),  # Index starts from 1
            fontsize=8,
            ha="center",
            va="center",
            color="red",
        )

    # Add title and labels (optional)
    ax.set_title("Subpolygons Split from Original Polygon", fontsize=14)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    # Save the plot to a file
    plt.savefig(
        f"{area_name}.png", dpi=300, bbox_inches="tight"
    )  # Adjust DPI for quality
    plt.close()  # Close the plot to free up resources

    print(f"Plot saved as {area_name}")


if __name__ == "__main__":
    # Define area
    polygon_coords = [[9.315779527110637, 7.393010160878238], [9.212354095738087, 7.422899704036905], [9.15655254457093, 7.3876407609333254], [9.113452278927843, 7.498454582116002], [9.051555627285737, 7.544347174727012], [9.070899478728172, 7.581844780884786], [9.013417555981286, 7.652922336286396], [8.987437145592784, 7.632214703035087], [8.90727309655127, 7.561137151064379], [8.812930407465702, 7.341253775066354], [8.817126195492317, 7.228062066061584], [8.894442631921066, 7.047486861826742], [8.93368935292086, 7.025817837318561], [9.022870702855295, 7.029429341403257], [9.272460574752747, 7.167870331316638], [9.325921197884986, 7.3520570396361755], [9.322357410655613, 7.390579749872942]]
    
    area_name = "abuja"

    # Define number of splits
    # NOTE: 8 is the maximum number os instances to spin-up on GCE, in parallel
    nr_splits = 20

    # Split area
    split_area(polygon_coords, nr_splits, area_name)

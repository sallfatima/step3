import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
from shapely.geometry import Polygon, box, MultiPolygon
from typing import List

def split_area_equivalent_area(coords: List[List[float]], n_splits: int, area_name: str):
    polygon = Polygon(coords)

    if not polygon.is_valid:
        polygon = polygon.buffer(0)

    minx, miny, maxx, maxy = polygon.bounds
    best_x, best_y, best_ratio = None, None, float('inf')

    # Trouver la meilleure grille (cellules les plus carrées)
    for x in range(1, n_splits + 1):
        y = int(np.ceil(n_splits / x))
        if x * y >= n_splits:
            cell_width = (maxx - minx) / x
            cell_height = (maxy - miny) / y
            ratio = max(cell_width / cell_height, cell_height / cell_width)
            if ratio < best_ratio:
                best_ratio = ratio
                best_x, best_y = x, y

    x_splits, y_splits = best_x, best_y
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

    # Intersections
    raw_subpolygons = [polygon.intersection(cell) for cell in grid if polygon.intersects(cell)]

    subpolygons = []
    for geom in raw_subpolygons:
        if isinstance(geom, Polygon):
            subpolygons.append(geom)
        elif isinstance(geom, MultiPolygon):
            subpolygons.extend([p for p in geom.geoms if not p.is_empty])

    if len(subpolygons) < n_splits:
        print(f"⚠️ Seulement {len(subpolygons)} sous-polygones valides générés (demande = {n_splits})")

    subpolygons = subpolygons[:n_splits]  # couper à n_splits

    gdf = gpd.GeoDataFrame(geometry=subpolygons, crs="EPSG:4326")
    gdf.to_file(f"{area_name}.geojson", driver="GeoJSON")

    fig, ax = plt.subplots(figsize=(8, 8))
    gdf.plot(ax=ax, color="lightblue", edgecolor="black")

    for idx, row in gdf.iterrows():
        centroid = row.geometry.centroid
        ax.text(centroid.x, centroid.y, str(idx + 1), fontsize=8, ha="center", va="center", color="red")

    ax.set_title("Découpage équivalent en surface")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    plt.savefig(f"{area_name}.png", dpi=300, bbox_inches="tight")
    plt.close()

    print(f"✅ Plot and GeoJSON saved: {area_name}.png & {area_name}.geojson")


if __name__ == "__main__":
    # Define area
    polygon_coords = [[9.069425802675344,7.473749622295586],[9.09792529103197,7.473749622295586],[9.09792529103197,7.451353033046735],[9.069425802675344,7.451353033046735],[9.069425802675344,7.473749622295586]]
    area_name = "abuja_custom25_4"

    # Define number of splits
    # NOTE: 8 is the maximum number os instances to spin-up on GCE, in parallel
    nr_splits = 12

    # Split area
    split_area_equivalent_area(polygon_coords, nr_splits, area_name)


    # Define area
  



    # # Define number of splits
    # # NOTE: 8 is the maximum number os instances to spin-up on GCE, in parallel
    # nr_splits = 12

    # # Split area
    # split_area_equivalent_area(polygon_coords, nr_splits, area_name)

    # polygon_coords = [[9.069425802675344,7.473749622295586],[9.040926314318718,7.473749622295586],[9.040926314318718,7.496146211544437],[9.069425802675344,7.496146211544437],[9.069425802675344,7.473749622295586]]

    # area_name = "abuja_custom19_9"

    # # Define number of splits
    # # NOTE: 8 is the maximum number os instances to spin-up on GCE, in parallel
    # nr_splits = 12

    # # Split area
    # split_area_equivalent_area(polygon_coords, nr_splits, area_name)


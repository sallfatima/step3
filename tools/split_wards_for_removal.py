# scripts/split_wards_for_removal.py
from typing import List
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
from shapely.geometry import Polygon, box
import json
import os

def split_area_for_removal(coords: List[List[float]], n_splits: int, ward_num: int, output_dir: str = "ward_subzones"):
    """
    Splits area defined by coordinates into n_splits and saves each subzone separately
    Args:
        coords: list of (lat, lon) lists defining a polygon
        n_splits: number of splits (default 10)
        ward_num: ward number
        output_dir: directory to save subzone files
    Returns:
        List of subzone filenames
    """
    os.makedirs(output_dir, exist_ok=True)
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

    # Limit to n_splits
    subpolygons = subpolygons[:n_splits]

    # Save each subzone individually
    subzone_files = []
    for idx, subpoly in enumerate(subpolygons):
        subzone_num = idx + 1
        
        # Create GeoDataFrame for this subzone
        sub_gdf = gpd.GeoDataFrame(
            [{"ward": ward_num, "subzone": subzone_num}], 
            geometry=[subpoly]
        )
        
        # Save subzone
        filename = f"poly_w{ward_num}_s{subzone_num}.geojson"
        filepath = os.path.join(output_dir, filename)
        sub_gdf.to_file(filepath, driver="GeoJSON")
        subzone_files.append(filename)
        
    # Create visualization
    gdf = gpd.GeoDataFrame({"geometry": subpolygons})
    fig, ax = plt.subplots(figsize=(8, 8))
    gdf.plot(ax=ax, color="lightblue", edgecolor="black")

    # Add polygon indices at centroids
    for idx, row in gdf.iterrows():
        centroid = row.geometry.centroid
        ax.text(
            centroid.x, centroid.y,
            str(idx + 1),
            fontsize=8, ha="center", va="center", color="red",
        )

    ax.set_title(f"Ward {ward_num} - Split into {len(subpolygons)} subzones", fontsize=14)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    # Save the plot
    plt.savefig(f"ward_{ward_num}_split.png", dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Ward {ward_num}: Created {len(subpolygons)} subzones")
    return subzone_files

def process_all_wards_and_generate_config():
    """
    Process all ward polygon files and generate Terraform configuration
    """
    import glob
    
    # Configuration pour Terraform
    terraform_config = {}
    
    # Traiter chaque fichier polygon_ward_X.geojson
    for ward_num in range(1, 136):  # 135 wards
        polygon_file = f"polygon_ward_{ward_num}.geojson"
        
        if os.path.exists(polygon_file):
            # Lire le polygone du ward
            with open(polygon_file, 'r') as f:
                ward_data = json.load(f)
            
            # Extraire les coordonnées
            if ward_data['type'] == 'FeatureCollection':
                coords = ward_data['features'][0]['geometry']['coordinates'][0]
            else:
                coords = ward_data['geometry']['coordinates'][0]
            
            # Diviser en 10 subzones
            subzone_files = split_area_for_removal(coords, n_splits=10, ward_num=ward_num)
            
            # Ajouter à la configuration Terraform
            for idx, subzone_file in enumerate(subzone_files):
                subzone_num = idx + 1
                job_name = f"joh-w{ward_num}-s{subzone_num}"
                
                terraform_config[job_name] = [
                    f"area.name=africa/south_africa/johannesburg/ward_{ward_num}/subzone_{subzone_num}",
                    "removal_export=[location_removal]",
                    f"annotations_filename=annotations_ward_{ward_num}.json",
                    f"custom_polygon_filename={subzone_file}"
                ]
    
    # Écrire la configuration Terraform
    with open('terraform.auto.tfvars', 'w') as f:
        f.write("cli_args_per_job = {\n")
        for job_name, config in terraform_config.items():
            f.write(f'  "{job_name}" = {json.dumps(config)},\n')
        f.write("}\n")
    
    print(f"\nGenerated Terraform configuration with {len(terraform_config)} jobs")

if __name__ == "__main__":
    process_all_wards_and_generate_config()# scripts/split_wards_for_removal.py
from typing import List
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
from shapely.geometry import Polygon, box
import json
import os

def split_area_for_removal(coords: List[List[float]], n_splits: int, ward_num: int, output_dir: str = "ward_subzones"):
    """
    Splits area defined by coordinates into n_splits and saves each subzone separately
    Args:
        coords: list of (lat, lon) lists defining a polygon
        n_splits: number of splits (default 10)
        ward_num: ward number
        output_dir: directory to save subzone files
    Returns:
        List of subzone filenames
    """
    os.makedirs(output_dir, exist_ok=True)
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

    # Limit to n_splits
    subpolygons = subpolygons[:n_splits]

    # Save each subzone individually
    subzone_files = []
    for idx, subpoly in enumerate(subpolygons):
        subzone_num = idx + 1
        
        # Create GeoDataFrame for this subzone
        sub_gdf = gpd.GeoDataFrame(
            [{"ward": ward_num, "subzone": subzone_num}], 
            geometry=[subpoly]
        )
        
        # Save subzone
        filename = f"poly_w{ward_num}_s{subzone_num}.geojson"
        filepath = os.path.join(output_dir, filename)
        sub_gdf.to_file(filepath, driver="GeoJSON")
        subzone_files.append(filename)
        
    # Create visualization
    gdf = gpd.GeoDataFrame({"geometry": subpolygons})
    fig, ax = plt.subplots(figsize=(8, 8))
    gdf.plot(ax=ax, color="lightblue", edgecolor="black")

    # Add polygon indices at centroids
    for idx, row in gdf.iterrows():
        centroid = row.geometry.centroid
        ax.text(
            centroid.x, centroid.y,
            str(idx + 1),
            fontsize=8, ha="center", va="center", color="red",
        )

    ax.set_title(f"Ward {ward_num} - Split into {len(subpolygons)} subzones", fontsize=14)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    # Save the plot
    plt.savefig(f"ward_{ward_num}_split.png", dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Ward {ward_num}: Created {len(subpolygons)} subzones")
    return subzone_files

def process_all_wards_and_generate_config():
    """
    Process all ward polygon files and generate Terraform configuration
    """
    import glob
    
    # Configuration pour Terraform
    terraform_config = {}
    
    # Traiter chaque fichier polygon_ward_X.geojson
    for ward_num in range(1, 136):  # 135 wards
        polygon_file = f"polygon_ward_{ward_num}.geojson"
        
        if os.path.exists(polygon_file):
            # Lire le polygone du ward
            with open(polygon_file, 'r') as f:
                ward_data = json.load(f)
            
            # Extraire les coordonnées
            if ward_data['type'] == 'FeatureCollection':
                coords = ward_data['features'][0]['geometry']['coordinates'][0]
            else:
                coords = ward_data['geometry']['coordinates'][0]
            
            # Diviser en 10 subzones
            subzone_files = split_area_for_removal(coords, n_splits=10, ward_num=ward_num)
            
            # Ajouter à la configuration Terraform
            for idx, subzone_file in enumerate(subzone_files):
                subzone_num = idx + 1
                job_name = f"joh-w{ward_num}-s{subzone_num}"
                
                terraform_config[job_name] = [
                    f"area.name=africa/south_africa/johannesburg/ward_{ward_num}/subzone_{subzone_num}",
                    "removal_export=[location_removal]",
                    f"annotations_filename=annotations_ward_{ward_num}.json",
                    f"custom_polygon_filename={subzone_file}"
                ]
    
    # Écrire la configuration Terraform
    with open('terraform.auto.tfvars', 'w') as f:
        f.write("cli_args_per_job = {\n")
        for job_name, config in terraform_config.items():
            f.write(f'  "{job_name}" = {json.dumps(config)},\n')
        f.write("}\n")
    
    print(f"\nGenerated Terraform configuration with {len(terraform_config)} jobs")

if __name__ == "__main__":
    process_all_wards_and_generate_config()
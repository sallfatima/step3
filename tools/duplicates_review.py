from typing import List

import numpy as np
import pandas as pd
import tqdm
from geopy.distance import geodesic
from joblib import Parallel, delayed
from sklearn.cluster import KMeans
from tqdm import tqdm


def cluster_points_kmeans(coords, n_clusters: int):
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    assigned_clusters = kmeans.fit_predict(coords)
    return assigned_clusters


def add_cluster(file_path: str, n_clusters: int) -> pd.DataFrame:
    # Load the Excel file
    df = pd.read_csv(file_path)

    # Extract latitude and longitude columns
    coords = df[["Latitude", "Longitude"]].to_numpy()

    # Apply KMeans clustering
    assigned_clusters = cluster_points_kmeans(coords, n_clusters)

    # Add the cluster column to the dataframe
    df["cluster"] = assigned_clusters

    # Save the updated dataframe to a new Excel file
    return df


# Optimized function using parallel processing
def shops_within_radius(df: pd.DataFrame, coords, radius_m: int):
    total_shops = len(coords)
    shops_with_neighbors = set()  # To store unique shops with neighbors

    def process_shop(i):
        """Check if a shop has at least one neighbor within the radius."""
        local_neighbors_to_check = set()  # Store neighbors for this shop

        for j in range(i + 1, total_shops):
            if geodesic(coords[i][:2], coords[j][:2]).meters <= radius_m:
                current_heading, current_side = coords[i][2:]
                neighbor_heading, neighbor_side = coords[j][2:]
                # If the heading is the same, add shop only if on the same side
                if current_heading == neighbor_heading:
                    if current_side == neighbor_side:
                        local_neighbors_to_check.add(j)
                else:
                    # If the heading is different, add any
                    local_neighbors_to_check.add(j)

        if local_neighbors_to_check:
            return i, local_neighbors_to_check  # Return shop index and its neighbors
        return None  # No neighbors found

    # Use joblib to parallelize computations
    result_indices = Parallel(n_jobs=-1)(
        delayed(process_shop)(i)
        for i in tqdm(range(total_shops), desc=f"Radius {radius_m}m")
    )

    # Process results
    for result in result_indices:
        if result is not None:
            shop_index, neighbor_indices = result
            shops_with_neighbors.add(shop_index)  # Add main shop
            shops_with_neighbors.update(neighbor_indices)  # Add all neighbors

    # Convert shops_with_neighbors to a sorted list
    shops_with_neighbors = sorted(shops_with_neighbors)

    # Create a DataFrame of shops with at least one neighbor
    shops_with_neighbors_df = df.iloc[shops_with_neighbors].copy()

    return (
        (len(shops_with_neighbors) / total_shops) * 100,
        len(shops_with_neighbors),
        shops_with_neighbors_df,
    )


def create_duplicate_files(csv_files: List[str], pattern: str, radiuses: List[int]):

    # Create dataframe from all files provided
    df = pd.concat((pd.read_csv(file) for file in csv_files), ignore_index=True)

    # Extract values using regex
    df[
        [
            "extracted_Latitude",
            "extracted_Longitude",
            "extracted_heading_index",
            "extracted_side_index",
        ]
    ] = df["PhotoURL"].str.extract(pattern)

    # Convert lat/lon columns to a NumPy array
    coords = (
        df[
            [
                "extracted_Latitude",
                "extracted_Longitude",
                "extracted_heading_index",
                "extracted_side_index",
            ]
        ]
        .astype(float)
        .to_numpy()
    )

    # Compute percentages and save shops with neighbors
    for radius in list(radiuses):
        percentage, shop_count, shops_with_neighbors_df = shops_within_radius(
            df, coords, radius
        )
        print(
            f"Percentage of shops with at least one neighbor within {radius}m: {percentage:.2f}% OR {shop_count} shops"
        )

        shops_with_neighbors_df = shops_with_neighbors_df.drop(
            columns=[
                "extracted_Latitude",
                "extracted_Longitude",
                "extracted_heading_index",
                "extracted_side_index",
            ]
        )

        # Save to CSV
        shops_with_neighbors_df.to_csv(
            f"shops_with_neighbors_{radius}m.csv", index=False
        )

        # Save the DataFrame of shops with neighbors for the current radius
        # (batch size of 2000, to fit in one Google My Maps csv upload)
        batch_size = 2000
        batches = [
            shops_with_neighbors_df[i : i + batch_size]
            for i in range(0, len(shops_with_neighbors_df), batch_size)
        ]

        for i, batch in enumerate(batches):
            # Save to CSV
            batch.to_csv(f"shops_with_neighbors_{radius}m_{i}.csv", index=False)
            print(
                f"Shops with neighbors within {radius}m saved to 'shops_with_neighbors_{radius}m_{i}.csv'"
            )


def add_clusters(n_clusters: int, radius: int):
    # Apply clustering and save the results
    df = add_cluster(f"shops_with_neighbors_{radius}m.csv", n_clusters)

    # Compute percentages and save shops with neighbors
    for cluster in df["cluster"].unique():
        df_with_cluster = df[df["cluster"] == cluster]
        df_with_cluster.to_csv(
            f"shops_with_neighbors_{radius}m_wit_clusters{cluster}.csv", index=False
        )


if __name__ == "__main__":

    # Select CSV files to include in duplicate analysis
    csv_files = []

    # Pattern for the filenames in CSV, to extract latitude, longitude, heading and side
    name_pattern = (
        r"roboflow_annotated_([-+]?\d*\.?\d+)_([-+]?\d*\.?\d+)_(-?\d+)_(-?\d+)"
    )

    # Radiuses (in m) to find the duplicate for (usually a list formed by 5, 10, 15, 20)
    to_compute_radiuses = [15]

    # Radius to look for duplicates (choose from to_compute_radiuses values)
    radius = 15

    # How many annotators will work for this
    n_clusters = 10

    # Create possible duplicate files
    create_duplicate_files(csv_files, name_pattern, to_compute_radiuses)

    # Cluster the duplicates for the annotators
    add_clusters(n_clusters, radius)

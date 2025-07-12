#!/usr/bin/env python3
"""
Filter building data for entire city or country
Much simpler than per-ward filtering!
"""
import pandas as pd
import gzip
import os
import sys
import subprocess
from typing import Tuple


def filter_buildings_for_area(
    area_name: str,
    area_type: str,
    bbox: Tuple[float, float, float, float],
    bucket_name: str = "lengo-geomapping"
) -> str:
    """
    Filter building data for a city or country
    
    Args:
        area_name: e.g., "johannesburg" or "south_africa"
        area_type: "city" or "country"
        bbox: (min_lat, min_lon, max_lat, max_lon)
        bucket_name: GCS bucket name
    
    Returns:
        Path to filtered file
    """
    min_lat, min_lon, max_lat, max_lon = bbox
    
    # Paths
    original_file = f"gs://{bucket_name}/buildings_database/africa/south_africa/open_buildings_v3_polygons_ne_110m.csv.gz"
    filtered_filename = f"buildings_filtered_{area_name}.csv.gz"
    gcs_filtered_path = f"gs://{bucket_name}/buildings_database/africa/south_africa/filtered/{filtered_filename}"
    
    print(f"\n{'='*60}")
    print(f"BUILDING DATA FILTER - {area_type.upper()} LEVEL")
    print(f"{'='*60}")
    print(f"Area: {area_name}")
    print(f"Type: {area_type}")
    print(f"Bbox: {bbox}")
    print(f"Original file: {original_file}")
    print(f"Output file: {gcs_filtered_path}")
    print(f"{'='*60}\n")
    
    # Check if filtered file already exists
    check_cmd = ['gcloud', 'storage', 'ls', gcs_filtered_path]
    result = subprocess.run(check_cmd, capture_output=True)
    if result.returncode == 0:
        print(f"✓ Filtered file already exists: {gcs_filtered_path}")
        # Get file size
        size_cmd = ['gcloud', 'storage', 'ls', '-l', gcs_filtered_path]
        size_result = subprocess.run(size_cmd, capture_output=True, text=True)
        print(f"  Size info: {size_result.stdout.strip()}")
        return gcs_filtered_path
    
    # Download original file
    print("1. Downloading original building file (6GB)...")
    print("   This will take 5-10 minutes...")
    temp_original = "temp_buildings_original.csv.gz"
    
    # Check if already downloaded
    if os.path.exists(temp_original):
        print("   ✓ Using existing download")
    else:
        download_cmd = ['gcloud', 'storage', 'cp', original_file, temp_original]
        subprocess.run(download_cmd, check=True)
        print(f"   ✓ Downloaded to {temp_original}")
    
    # Filter buildings
    print(f"\n2. Filtering buildings for {area_type}: {area_name}...")
    filtered_buildings = []
    total_buildings = 0
    kept_buildings = 0
    chunk_size = 100000
    
    with gzip.open(temp_original, 'rt') as f:
        # Read header to identify columns
        header = f.readline().strip().split(',')
        print(f"   Columns found: {header[:5]}...")  # Show first 5 columns
        
        # Find lat/lon columns
        lat_col_idx = None
        lon_col_idx = None
        for i, col in enumerate(header):
            if 'lat' in col.lower():
                lat_col_idx = i
            if 'lon' in col.lower() or 'lng' in col.lower():
                lon_col_idx = i
        
        if lat_col_idx is None or lon_col_idx is None:
            raise ValueError(f"Could not find lat/lon columns in: {header}")
        
        print(f"   Using columns: {header[lat_col_idx]} (lat), {header[lon_col_idx]} (lon)")
        
        # Reset file pointer
        f.seek(0)
        
        # Process in chunks
        for chunk_num, chunk in enumerate(pd.read_csv(f, chunksize=chunk_size)):
            total_buildings += len(chunk)
            
            # Get column names
            lat_col = chunk.columns[lat_col_idx]
            lon_col = chunk.columns[lon_col_idx]
            
            # Filter by bbox
            mask = (
                (chunk[lat_col] >= min_lat) & 
                (chunk[lat_col] <= max_lat) &
                (chunk[lon_col] >= min_lon) & 
                (chunk[lon_col] <= max_lon)
            )
            
            filtered_chunk = chunk[mask]
            if len(filtered_chunk) > 0:
                filtered_buildings.append(filtered_chunk)
                kept_buildings += len(filtered_chunk)
            
            # Progress
            if (chunk_num + 1) % 10 == 0:
                percentage = (kept_buildings / total_buildings) * 100 if total_buildings > 0 else 0
                print(f"   Processed {total_buildings:,} buildings, kept {kept_buildings:,} ({percentage:.1f}%)")
    
    # Save filtered data
    print(f"\n3. Saving filtered data...")
    if filtered_buildings:
        result_df = pd.concat(filtered_buildings, ignore_index=True)
        print(f"   Total buildings in {area_name}: {len(result_df):,} (from {total_buildings:,})")
        print(f"   Reduction: {((1 - len(result_df)/total_buildings) * 100):.1f}%")
        
        # Save locally
        result_df.to_csv(filtered_filename, compression='gzip', index=False)
        local_size_mb = os.path.getsize(filtered_filename) / (1024 * 1024)
        print(f"   ✓ Saved locally: {filtered_filename} ({local_size_mb:.1f} MB)")
        
        # Upload to GCS
        print(f"\n4. Uploading to GCS...")
        upload_cmd = ['gcloud', 'storage', 'cp', filtered_filename, gcs_filtered_path]
        subprocess.run(upload_cmd, check=True)
        print(f"   ✓ Uploaded to: {gcs_filtered_path}")
        
        # Cleanup local file
        os.remove(filtered_filename)
    else:
        print("   ⚠️  No buildings found in bbox!")
    
    # Keep the original download for next run
    print(f"\n✓ Complete! Keeping original download for future use.")
    
    return gcs_filtered_path


def main():
    """Main function with bbox for cities and country"""
    
    # Bounding boxes
    AREAS = {
        # Cities
        "johannesburg": {
            "type": "city",
            "bbox": (-26.4179, 27.6546, -25.8650, 28.4018),  # Greater Johannesburg
            "description": "Greater Johannesburg metropolitan area"
        },
        "cape_town": {
            "type": "city", 
            "bbox": (-34.3588, 18.3071, -33.4712, 19.0046),  # Cape Town metro
            "description": "Cape Town metropolitan area"
        },
        "durban": {
            "type": "city",
            "bbox": (-30.1798, 30.6924, -29.4893, 31.4409),  # Durban metro
            "description": "Durban metropolitan area"
        },
        
        # Country
        "south_africa": {
            "type": "country",
            "bbox": (-34.8333, 16.4500, -22.1250, 32.8917),  # Entire South Africa
            "description": "Entire South Africa"
        }
    }
    
    # Get area from command line
    if len(sys.argv) > 1:
        area_name = sys.argv[1].lower()
    else:
        print("Usage: python filter_buildings.py <area_name>")
        print("\nAvailable areas:")
        for name, info in AREAS.items():
            print(f"  - {name}: {info['description']}")
        sys.exit(1)
    
    if area_name not in AREAS:
        print(f"Error: Unknown area '{area_name}'")
        print(f"\nAvailable areas:")
        for name, info in AREAS.items():
            print(f"  - {name}: {info['description']}")
        sys.exit(1)
    
    area_info = AREAS[area_name]
    
    # Confirm with user for large areas
    if area_info["type"] == "country":
        print(f"\n⚠️  WARNING: Filtering for entire {area_name} will create a large file!")
        print(f"Estimated size: 1-2 GB")
        response = input("Continue? (y/n): ")
        if response.lower() != 'y':
            print("Cancelled.")
            sys.exit(0)
    
    try:
        filtered_path = filter_buildings_for_area(
            area_name, 
            area_info["type"],
            area_info["bbox"]
        )
        
        print(f"\n{'='*60}")
        print(f"SUCCESS!")
        print(f"{'='*60}")
        print(f"Filtered file: {filtered_path}")
        print(f"\nTo use in your pipeline, update your code to use:")
        print(f"  area_name = '{area_name}'")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
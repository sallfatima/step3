#!/usr/bin/env python3
"""
Download South Africa building data from Google Open Buildings
"""
import os
import subprocess
import pandas as pd
import gzip
from typing import List


def download_south_africa_buildings():
    """
    Download building data for South Africa from Google Open Buildings
    """
    
    print("\n" + "="*60)
    print("DOWNLOADING SOUTH AFRICA BUILDINGS")
    print("="*60)
    
    # Google Open Buildings URLs for South Africa
    # These are organized by S2 cells
    sa_urls = [
        # Western Cape (Cape Town area)
        "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/0d7_buildings.csv.gz",
        "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/0d5_buildings.csv.gz",
        
        # Gauteng (Johannesburg area) - THESE ARE THE IMPORTANT ONES
        "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/0e1_buildings.csv.gz",
        "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/0e3_buildings.csv.gz",
        "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/0df_buildings.csv.gz",
        
        # KwaZulu-Natal (Durban area)
        "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/0e5_buildings.csv.gz",
        "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/0e7_buildings.csv.gz",
        
        # Eastern Cape
        "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/0d9_buildings.csv.gz",
        "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/0db_buildings.csv.gz",
        
        # Add more as needed...
    ]
    
    # Alternative: Get the full list from the CSV
    print("\n1. Downloading S2 cell list for Africa...")
    africa_csv_url = "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/africa_tiles.csv"
    
    try:
        # Download the tile list
        subprocess.run(['curl', '-o', 'africa_tiles.csv', africa_csv_url], check=True)
        
        # Read and filter for South Africa
        tiles_df = pd.read_csv('africa_tiles.csv')
        print(f"   Found {len(tiles_df)} total tiles for Africa")
        
        # Filter for South Africa (approximate bounds)
        sa_tiles = tiles_df[
            (tiles_df['latitude'] >= -35) & 
            (tiles_df['latitude'] <= -22) &
            (tiles_df['longitude'] >= 16) & 
            (tiles_df['longitude'] <= 33)
        ]
        
        print(f"   Found {len(sa_tiles)} tiles for South Africa")
        
        # Get URLs for SA tiles
        sa_urls = []
        for _, tile in sa_tiles.iterrows():
            tile_id = tile['tile_id'] if 'tile_id' in tile else tile.iloc[0]
            url = f"https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/{tile_id}_buildings.csv.gz"
            sa_urls.append(url)
            
    except Exception as e:
        print(f"   Could not download tile list: {e}")
        print("   Using predefined list of tiles...")
    
    # Download all tiles
    print(f"\n2. Downloading {len(sa_urls)} tiles for South Africa...")
    
    downloaded_files = []
    for i, url in enumerate(sa_urls[:10]):  # Limit to first 10 for testing
        filename = url.split('/')[-1]
        print(f"   [{i+1}/{len(sa_urls[:10])}] Downloading {filename}...")
        
        try:
            subprocess.run(['curl', '-o', filename, url], check=True)
            downloaded_files.append(filename)
        except:
            print(f"   ⚠️  Failed to download {filename}")
    
    print(f"\n3. Combining {len(downloaded_files)} files...")
    
    # Combine all files
    all_buildings = []
    total_buildings = 0
    
    for filename in downloaded_files:
        try:
            df = pd.read_csv(filename, compression='gzip')
            all_buildings.append(df)
            total_buildings += len(df)
            print(f"   {filename}: {len(df):,} buildings")
        except Exception as e:
            print(f"   ⚠️  Error reading {filename}: {e}")
    
    if all_buildings:
        print(f"\n4. Merging all data...")
        combined_df = pd.concat(all_buildings, ignore_index=True)
        print(f"   Total buildings: {len(combined_df):,}")
        
        # Save as the expected filename
        output_file = "open_buildings_v3_polygons_ne_110m.csv.gz"
        print(f"\n5. Saving to {output_file}...")
        combined_df.to_csv(output_file, compression='gzip', index=False)
        
        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
        print(f"   ✓ Saved! Size: {file_size_mb:.1f} MB")
        
        # Upload to GCS
        print(f"\n6. Uploading to GCS...")
        gcs_path = "gs://lengo-geomapping/buildings_database/africa/south_africa/open_buildings_v3_polygons_ne_110m.csv.gz"
        
        upload_cmd = ['gcloud', 'storage', 'cp', output_file, gcs_path]
        try:
            subprocess.run(upload_cmd, check=True)
            print(f"   ✓ Uploaded to {gcs_path}")
        except:
            print(f"   ⚠️  Failed to upload. You can manually upload with:")
            print(f"   gcloud storage cp {output_file} {gcs_path}")
        
        # Clean up downloaded files
        print("\n7. Cleaning up temporary files...")
        for filename in downloaded_files:
            if os.path.exists(filename):
                os.remove(filename)
        
        return output_file
    
    else:
        print("\n⚠️  No buildings downloaded!")
        return None


def quick_johannesburg_only():
    """
    Quick version: Download only Johannesburg area buildings
    """
    print("\n" + "="*60)
    print("QUICK DOWNLOAD - JOHANNESBURG ONLY")
    print("="*60)
    
    # S2 cells that cover Johannesburg
    jhb_urls = [
        "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/0e1_buildings.csv.gz",
        "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/0e3_buildings.csv.gz",
        "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/0df_buildings.csv.gz",
    ]
    
    print("Downloading Johannesburg area tiles...")
    
    all_dfs = []
    for url in jhb_urls:
        filename = url.split('/')[-1]
        print(f"\nDownloading {filename}...")
        
        # Download
        subprocess.run(['curl', '-O', url], check=True)
        
        # Read and filter to Johannesburg bbox
        df = pd.read_csv(filename, compression='gzip')
        
        # Filter to Johannesburg area
        jhb_df = df[
            (df['latitude'] >= -26.5) & 
            (df['latitude'] <= -25.8) &
            (df['longitude'] >= 27.6) & 
            (df['longitude'] <= 28.5)
        ]
        
        print(f"  Total buildings: {len(df):,}")
        print(f"  Johannesburg buildings: {len(jhb_df):,}")
        
        if len(jhb_df) > 0:
            all_dfs.append(jhb_df)
        
        # Clean up
        os.remove(filename)
    
    # Combine
    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        print(f"\nTotal Johannesburg buildings: {len(combined):,}")
        
        # Save
        output = "johannesburg_buildings.csv.gz"
        combined.to_csv(output, compression='gzip', index=False)
        print(f"Saved to {output}")
        
        return output
    
    return None


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "quick":
        # Quick mode - Johannesburg only
        quick_johannesburg_only()
    else:
        # Full South Africa download
        download_south_africa_buildings()
#!/usr/bin/env python3
"""
Script to check the format and content of the building data
"""
import pandas as pd
import gzip


def check_building_data():
    """Check first few rows of building data to understand format"""
    
    print("\n" + "="*60)
    print("CHECKING BUILDING DATA FORMAT")
    print("="*60)
    
    # Read just the first 1000 rows
    print("\nReading first 1000 rows...")
    
    with gzip.open('temp_buildings_original.csv.gz', 'rt') as f:
        df_sample = pd.read_csv(f, nrows=1000)
    
    print(f"\nColumns: {list(df_sample.columns)}")
    print(f"\nShape: {df_sample.shape}")
    
    # Show first 5 rows
    print("\nFirst 5 rows:")
    print(df_sample.head())
    
    # Check latitude/longitude ranges
    print("\nLatitude range:")
    print(f"  Min: {df_sample['latitude'].min()}")
    print(f"  Max: {df_sample['latitude'].max()}")
    
    print("\nLongitude range:")
    print(f"  Min: {df_sample['longitude'].min()}")
    print(f"  Max: {df_sample['longitude'].max()}")
    
    # Check for South Africa coordinates
    # South Africa is roughly between:
    # Latitude: -35 to -22
    # Longitude: 16 to 33
    
    sa_mask = (
        (df_sample['latitude'] >= -35) & 
        (df_sample['latitude'] <= -22) &
        (df_sample['longitude'] >= 16) & 
        (df_sample['longitude'] <= 33)
    )
    
    sa_buildings = df_sample[sa_mask]
    print(f"\nBuildings in South Africa bounds: {len(sa_buildings)}")
    
    if len(sa_buildings) > 0:
        print("\nSample South African buildings:")
        print(sa_buildings.head())
        
        # Check Johannesburg area specifically
        # Johannesburg: roughly -26.2 lat, 28.0 lon
        jhb_mask = (
            (df_sample['latitude'] >= -27) & 
            (df_sample['latitude'] <= -25) &
            (df_sample['longitude'] >= 27) & 
            (df_sample['longitude'] <= 29)
        )
        
        jhb_buildings = df_sample[jhb_mask]
        print(f"\nBuildings near Johannesburg: {len(jhb_buildings)}")
        
        if len(jhb_buildings) > 0:
            print("\nSample Johannesburg buildings:")
            print(jhb_buildings.head())
    
    # Check if coordinates might be swapped
    print("\n" + "-"*40)
    print("Checking if coordinates might be swapped...")
    
    # If we swap lat/lon, would we get South Africa?
    swapped_sa_mask = (
        (df_sample['longitude'] >= -35) & 
        (df_sample['longitude'] <= -22) &
        (df_sample['latitude'] >= 16) & 
        (df_sample['latitude'] <= 33)
    )
    
    swapped_sa_buildings = df_sample[swapped_sa_mask]
    print(f"Buildings in SA bounds with swapped coords: {len(swapped_sa_buildings)}")
    
    # Look for any negative latitudes (Southern hemisphere)
    negative_lat = df_sample[df_sample['latitude'] < 0]
    print(f"\nBuildings with negative latitude (Southern hemisphere): {len(negative_lat)}")
    
    if len(negative_lat) > 0:
        print("Sample:")
        print(negative_lat.head())
    
    # Summary statistics
    print("\n" + "-"*40)
    print("SUMMARY STATISTICS")
    print("-"*40)
    print("\nLatitude statistics:")
    print(df_sample['latitude'].describe())
    print("\nLongitude statistics:")
    print(df_sample['longitude'].describe())
    
    # Check a larger sample to find South Africa
    print("\n" + "-"*40)
    print("Scanning larger sample for South African buildings...")
    
    found_sa = False
    chunk_size = 100000
    chunks_to_check = 10
    
    with gzip.open('temp_buildings_original.csv.gz', 'rt') as f:
        for i in range(chunks_to_check):
            chunk = pd.read_csv(f, nrows=chunk_size, skiprows=i*chunk_size if i > 0 else None)
            
            sa_in_chunk = chunk[
                (chunk['latitude'] >= -35) & 
                (chunk['latitude'] <= -22) &
                (chunk['longitude'] >= 16) & 
                (chunk['longitude'] <= 33)
            ]
            
            if len(sa_in_chunk) > 0:
                print(f"\nFound {len(sa_in_chunk)} SA buildings in chunk {i+1}")
                print("Sample:")
                print(sa_in_chunk.head())
                found_sa = True
                break
    
    if not found_sa:
        print("\n⚠️  NO SOUTH AFRICAN BUILDINGS FOUND in first million rows!")
        print("This file might contain data for a different region.")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    check_building_data()
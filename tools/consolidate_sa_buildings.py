#!/usr/bin/env python3
"""
Consolidate South Africa building files from Google Open Buildings
"""
import pandas as pd
import glob
import os
import gzip
import numpy as np


def consolidate_building_files(
    input_dir: str = "/Users/fatousall/Documents/geo-mapping/tools/johannesburg_tuile",
    output_file: str = "south_africa_buildings_consolidated.csv.gz",
    filter_to_sa: bool = True
):
    """
    Consolidate multiple building CSV files into one optimized file
    
    Args:
        input_dir: Directory containing the building files
        output_file: Output consolidated file name
        filter_to_sa: Whether to filter to South Africa bounds only
    """
    print("\n" + "="*60)
    print("CONSOLIDATING SOUTH AFRICA BUILDING FILES")
    print("="*60)
    
    # Find all building files
    pattern = os.path.join(input_dir, "*_buildings.csv.gz")
    files = glob.glob(pattern)
    
    if not files:
        print(f"❌ No building files found in {input_dir}")
        print(f"   Looking for pattern: {pattern}")
        return None
    
    print(f"\nFound {len(files)} building files:")
    for f in sorted(files):
        size_mb = os.path.getsize(f) / (1024 * 1024)
        print(f"  - {os.path.basename(f)} ({size_mb:.1f} MB)")
    
    # South Africa bounds
    sa_bounds = {
        'min_lat': -34.8333,
        'max_lat': -22.1250,
        'min_lon': 16.4500,
        'max_lon': 32.8917
    }
    
    # Major cities for reference
    cities = {
        'Johannesburg': {'lat': -26.2041, 'lon': 28.0473, 'radius': 0.5},
        'Cape Town': {'lat': -33.9249, 'lon': 18.4241, 'radius': 0.5},
        'Durban': {'lat': -29.8587, 'lon': 31.0218, 'radius': 0.5},
        'Pretoria': {'lat': -25.7479, 'lon': 28.2293, 'radius': 0.3},
        'Port Elizabeth': {'lat': -33.9608, 'lon': 25.6022, 'radius': 0.3}
    }
    
    # Process files
    print(f"\n{'='*40}")
    print("PROCESSING FILES")
    print("="*40)
    
    all_buildings = []
    total_raw_buildings = 0
    city_counts = {city: 0 for city in cities}
    
    for i, file_path in enumerate(sorted(files)):
        print(f"\n[{i+1}/{len(files)}] Processing {os.path.basename(file_path)}...")
        
        try:
            # Read file
            df = pd.read_csv(file_path, compression='gzip')
            total_raw_buildings += len(df)
            print(f"  - Loaded {len(df):,} buildings")
            
            # Show coordinate ranges
            print(f"  - Lat range: {df['latitude'].min():.3f} to {df['latitude'].max():.3f}")
            print(f"  - Lon range: {df['longitude'].min():.3f} to {df['longitude'].max():.3f}")
            
            # Filter to South Africa bounds if requested
            if filter_to_sa:
                df_filtered = df[
                    (df['latitude'] >= sa_bounds['min_lat']) & 
                    (df['latitude'] <= sa_bounds['max_lat']) &
                    (df['longitude'] >= sa_bounds['min_lon']) & 
                    (df['longitude'] <= sa_bounds['max_lon'])
                ]
                print(f"  - After SA filter: {len(df_filtered):,} buildings ({len(df_filtered)/len(df)*100:.1f}%)")
                df = df_filtered
            
            # Count buildings near major cities
            for city, info in cities.items():
                city_mask = (
                    (df['latitude'] >= info['lat'] - info['radius']) & 
                    (df['latitude'] <= info['lat'] + info['radius']) &
                    (df['longitude'] >= info['lon'] - info['radius']) & 
                    (df['longitude'] <= info['lon'] + info['radius'])
                )
                city_count = city_mask.sum()
                city_counts[city] += city_count
                if city_count > 0:
                    print(f"  - Near {city}: {city_count:,} buildings")
            
            if len(df) > 0:
                all_buildings.append(df)
                
        except Exception as e:
            print(f"  ❌ Error processing {file_path}: {e}")
            continue
    
    # Combine all dataframes
    if not all_buildings:
        print("\n❌ No buildings found to consolidate!")
        return None
    
    print(f"\n{'='*40}")
    print("CONSOLIDATING DATA")
    print("="*40)
    
    print("\nCombining all dataframes...")
    consolidated_df = pd.concat(all_buildings, ignore_index=True)
    
    print(f"\nConsolidation complete:")
    print(f"  - Total buildings before filtering: {total_raw_buildings:,}")
    print(f"  - Total buildings after filtering: {len(consolidated_df):,}")
    print(f"  - Reduction: {(1 - len(consolidated_df)/total_raw_buildings)*100:.1f}%")
    
    # Remove exact duplicates if any
    print("\nChecking for duplicates...")
    original_count = len(consolidated_df)
    consolidated_df = consolidated_df.drop_duplicates()
    duplicate_count = original_count - len(consolidated_df)
    if duplicate_count > 0:
        print(f"  - Removed {duplicate_count:,} exact duplicates")
    else:
        print(f"  - No duplicates found")
    
    # Show final statistics
    print(f"\n{'='*40}")
    print("FINAL STATISTICS")
    print("="*40)
    
    print(f"\nTotal buildings: {len(consolidated_df):,}")
    print(f"\nCoordinate ranges:")
    print(f"  - Latitude: {consolidated_df['latitude'].min():.3f} to {consolidated_df['latitude'].max():.3f}")
    print(f"  - Longitude: {consolidated_df['longitude'].min():.3f} to {consolidated_df['longitude'].max():.3f}")
    
    print(f"\nBuildings by city:")
    for city, count in sorted(city_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  - {city}: {count:,} buildings")
    
    print(f"\nArea statistics:")
    print(f"  - Mean area: {consolidated_df['area_in_meters'].mean():.1f} m²")
    print(f"  - Median area: {consolidated_df['area_in_meters'].median():.1f} m²")
    print(f"  - Total area: {consolidated_df['area_in_meters'].sum()/1e6:.1f} km²")
    
    # Save consolidated file
    print(f"\n{'='*40}")
    print("SAVING CONSOLIDATED FILE")
    print("="*40)
    
    print(f"\nSaving to {output_file}...")
    consolidated_df.to_csv(output_file, compression='gzip', index=False)
    
    # Check file size
    output_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"✅ Saved successfully!")
    print(f"   - File: {output_file}")
    print(f"   - Size: {output_size_mb:.1f} MB")
    print(f"   - Buildings: {len(consolidated_df):,}")
    
    # Create a smaller Johannesburg-only file
    if city_counts.get('Johannesburg', 0) > 0:
        print(f"\nCreating Johannesburg-specific file...")
        jhb_df = consolidated_df[
            (consolidated_df['latitude'] >= -26.5) & 
            (consolidated_df['latitude'] <= -25.8) &
            (consolidated_df['longitude'] >= 27.6) & 
            (consolidated_df['longitude'] <= 28.5)
        ]
        
        jhb_file = "johannesburg_buildings_only.csv.gz"
        jhb_df.to_csv(jhb_file, compression='gzip', index=False)
        jhb_size_mb = os.path.getsize(jhb_file) / (1024 * 1024)
        
        print(f"✅ Created Johannesburg file:")
        print(f"   - File: {jhb_file}")
        print(f"   - Size: {jhb_size_mb:.1f} MB")
        print(f"   - Buildings: {len(jhb_df):,}")
    
    # Upload instructions
    print(f"\n{'='*40}")
    print("NEXT STEPS")
    print("="*40)
    
    print("\n1. Upload to GCS:")
    print(f"   gcloud storage cp {output_file} gs://lengo-geomapping/buildings_database/africa/south_africa/")
    
    print("\n2. Or replace the existing file:")
    print(f"   gcloud storage cp {output_file} gs://lengo-geomapping/buildings_database/africa/south_africa/open_buildings_v3_polygons_ne_110m.csv.gz")
    
    print("\n3. For Johannesburg only (smaller, faster):")
    print(f"   gcloud storage cp johannesburg_buildings_only.csv.gz gs://lengo-geomapping/buildings_database/africa/south_africa/")
    

    # Copier le fichier avec le nouveau nom
    return output_file


def validate_files(input_dir: str):
    """
    Validate that the files contain South African data
    """
    print("\n" + "="*60)
    print("VALIDATING BUILDING FILES")
    print("="*60)
    
    pattern = os.path.join(input_dir, "*_buildings.csv.gz")
    files = glob.glob(pattern)
    
    sa_files = []
    other_files = []
    
    for file_path in files:
        print(f"\nChecking {os.path.basename(file_path)}...")
        
        try:
            # Read first 1000 rows
            df_sample = pd.read_csv(file_path, compression='gzip', nrows=1000)
            
            # Check if it contains SA data
            sa_data = df_sample[
                (df_sample['latitude'] < -22) & 
                (df_sample['latitude'] > -35)
            ]
            
            if len(sa_data) > 0:
                print(f"  ✅ Contains South African data ({len(sa_data)} buildings in sample)")
                sa_files.append(file_path)
            else:
                print(f"  ❌ No South African data found")
                print(f"     Lat range: {df_sample['latitude'].min():.1f} to {df_sample['latitude'].max():.1f}")
                other_files.append(file_path)
                
        except Exception as e:
            print(f"  ⚠️  Error reading file: {e}")
    
    print(f"\n{'='*40}")
    print("SUMMARY")
    print("="*40)
    print(f"\nFiles with SA data: {len(sa_files)}")
    print(f"Files without SA data: {len(other_files)}")
    
    if other_files:
        print("\nFiles to exclude:")
        for f in other_files:
            print(f"  - {os.path.basename(f)}")
    
    return sa_files


if __name__ == "__main__":
    import sys
    
    # Default directory
    input_dir = "/Users/fatousall/Documents/geo-mapping/tools/johannesburg_tuile"
    
    # Check if custom directory provided
    if len(sys.argv) > 1:
        if sys.argv[1] == "validate":
            # Just validate files
            validate_files(input_dir)
        else:
            input_dir = sys.argv[1]
            consolidate_building_files(input_dir)
    else:
        # Run consolidation
        consolidate_building_files(input_dir)
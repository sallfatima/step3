#!/usr/bin/env python3
"""
Find the correct S2 tiles for Johannesburg
"""
import pandas as pd
import subprocess
import s2geometry as s2
import math


def lat_lng_to_s2_token(lat, lng, level=4):
    """Convert lat/lng to S2 cell token at given level"""
    try:
        # Create S2 point from lat/lng
        latlng = s2.S2LatLng.FromDegrees(lat, lng)
        cell_id = s2.S2CellId(latlng)
        
        # Get parent at desired level
        parent = cell_id.parent(level)
        
        # Get token (hex representation)
        token = parent.ToToken()
        
        return token
    except:
        # If s2geometry not installed, use alternative method
        return None


def find_tiles_alternative():
    """Alternative method using the CSV list from Google"""
    print("\n" + "="*60)
    print("FINDING S2 TILES FOR JOHANNESBURG")
    print("="*60)
    
    # Johannesburg coordinates
    jhb_bounds = {
        'min_lat': -26.5,
        'max_lat': -25.8,
        'min_lon': 27.6,
        'max_lon': 28.5
    }
    
    print(f"\nJohannesburg bounds:")
    print(f"  Latitude: {jhb_bounds['min_lat']} to {jhb_bounds['max_lat']}")
    print(f"  Longitude: {jhb_bounds['min_lon']} to {jhb_bounds['max_lon']}")
    
    # Download the complete tile list
    print("\n1. Downloading complete tile list...")
    csv_url = "https://sites.research.google/open-buildings/tiles.csv"
    
    try:
        subprocess.run(['curl', '-o', 'tiles.csv', csv_url], check=True)
    except:
        # Try alternative URL
        csv_url = "https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/tiles.csv"
        subprocess.run(['curl', '-o', 'tiles.csv', csv_url], check=True)
    
    # Read the CSV
    print("\n2. Reading tile data...")
    tiles_df = pd.read_csv('tiles.csv')
    print(f"   Total tiles: {len(tiles_df)}")
    print(f"   Columns: {list(tiles_df.columns)}")
    
    # Show sample
    print("\n   Sample data:")
    print(tiles_df.head())
    
    # Find column names (they might vary)
    lat_col = None
    lon_col = None
    tile_col = None
    
    for col in tiles_df.columns:
        if 'lat' in col.lower():
            lat_col = col
        elif 'lon' in col.lower() or 'lng' in col.lower():
            lon_col = col
        elif 'tile' in col.lower() or 'id' in col.lower():
            tile_col = col
    
    print(f"\n   Using columns: lat={lat_col}, lon={lon_col}, tile={tile_col}")
    
    # Filter for South Africa first
    print("\n3. Filtering for South Africa...")
    sa_tiles = tiles_df[
        (tiles_df[lat_col] >= -35) & 
        (tiles_df[lat_col] <= -22) &
        (tiles_df[lon_col] >= 16) & 
        (tiles_df[lon_col] <= 33)
    ]
    print(f"   South Africa tiles: {len(sa_tiles)}")
    
    # Filter for Johannesburg area (with buffer)
    print("\n4. Filtering for Johannesburg area...")
    buffer = 0.5  # Add buffer to ensure we don't miss edges
    jhb_tiles = sa_tiles[
        (sa_tiles[lat_col] >= jhb_bounds['min_lat'] - buffer) & 
        (sa_tiles[lat_col] <= jhb_bounds['max_lat'] + buffer) &
        (sa_tiles[lon_col] >= jhb_bounds['min_lon'] - buffer) & 
        (sa_tiles[lon_col] <= jhb_bounds['max_lon'] + buffer)
    ]
    
    print(f"   Johannesburg area tiles: {len(jhb_tiles)}")
    
    if len(jhb_tiles) > 0:
        print("\n5. Tiles covering Johannesburg:")
        for idx, row in jhb_tiles.iterrows():
            tile_id = row[tile_col]
            lat = row[lat_col]
            lon = row[lon_col]
            print(f"   - {tile_id} (center: {lat:.3f}, {lon:.3f})")
        
        # Generate download commands
        print("\n6. Download commands:")
        print("```bash")
        for idx, row in jhb_tiles.iterrows():
            tile_id = row[tile_col]
            url = f"https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/{tile_id}_buildings.csv.gz"
            print(f"curl -O {url}")
        print("```")
        
        # Save the list
        jhb_tiles.to_csv('johannesburg_tiles.csv', index=False)
        print(f"\n✓ Saved tile list to johannesburg_tiles.csv")
        
        return jhb_tiles
    
    else:
        print("\n⚠️  No tiles found for Johannesburg!")
        print("\nDebugging: Let's check some South African tiles...")
        
        # Show some SA tiles
        print("\nSample South African tiles:")
        print(sa_tiles.head(20))
        
        # Try to find tiles manually by checking coordinates
        print("\n7. Searching manually...")
        
        # Check tiles that might contain Johannesburg
        potential_tiles = []
        for idx, row in sa_tiles.iterrows():
            lat = row[lat_col]
            lon = row[lon_col]
            
            # Check if this tile might overlap with Johannesburg
            # S2 level 4 cells are quite large (~1000km across)
            if -30 <= lat <= -23 and 25 <= lon <= 30:
                potential_tiles.append(row)
        
        if potential_tiles:
            print(f"\nFound {len(potential_tiles)} potential tiles:")
            for row in potential_tiles[:10]:  # Show first 10
                tile_id = row[tile_col]
                lat = row[lat_col]
                lon = row[lon_col]
                print(f"   - {tile_id} (center: {lat:.3f}, {lon:.3f})")
        
        return None


def download_and_check_tiles():
    """Download tiles and check for Johannesburg buildings"""
    
    # These are S2 level 4 tiles that should contain South Africa
    # Based on S2 geometry, these should cover the region
    test_tiles = [
        "0e1", "0e3", "0e5", "0e7", "0e9", "0eb", "0ed", "0ef",
        "0f1", "0f3", "0f5", "0f7", "0f9", "0fb", "0fd", "0ff",
        "101", "103", "105", "107", "109", "10b", "10d", "10f"
    ]
    
    print("\n" + "="*60)
    print("CHECKING S2 TILES FOR JOHANNESBURG BUILDINGS")
    print("="*60)
    
    found_tiles = []
    
    for tile in test_tiles[:5]:  # Test first 5
        url = f"https://storage.googleapis.com/open-buildings-data/v3/polygons_s2_level_4_gzip/{tile}_buildings.csv.gz"
        filename = f"{tile}_buildings.csv.gz"
        
        print(f"\nChecking tile {tile}...")
        
        try:
            # Download just the first few KB to check
            subprocess.run(['curl', '-r', '0-1000000', '-o', f'sample_{filename}', url], 
                         capture_output=True, check=True)
            
            # Try to read sample
            try:
                df_sample = pd.read_csv(f'sample_{filename}', compression='gzip', nrows=1000)
                
                # Check coordinates
                lat_min, lat_max = df_sample['latitude'].min(), df_sample['latitude'].max()
                lon_min, lon_max = df_sample['longitude'].min(), df_sample['longitude'].max()
                
                print(f"  Lat range: {lat_min:.2f} to {lat_max:.2f}")
                print(f"  Lon range: {lon_min:.2f} to {lon_max:.2f}")
                
                # Check if it might contain Johannesburg
                if lat_max < -20 and lat_min > -35 and lon_max > 20 and lon_min < 35:
                    print(f"  ✓ Might contain South African data!")
                    
                    # Check specifically for Johannesburg
                    jhb_check = df_sample[
                        (df_sample['latitude'] >= -26.5) & 
                        (df_sample['latitude'] <= -25.8) &
                        (df_sample['longitude'] >= 27.6) & 
                        (df_sample['longitude'] <= 28.5)
                    ]
                    
                    if len(jhb_check) > 0:
                        print(f"  ✓✓ Contains Johannesburg buildings!")
                        found_tiles.append(tile)
                
            except:
                print(f"  ⚠️  Could not read sample")
            
            # Clean up
            subprocess.run(['rm', f'sample_{filename}'], capture_output=True)
            
        except:
            print(f"  ⚠️  Tile not found")
    
    if found_tiles:
        print(f"\n✓ Found {len(found_tiles)} tiles with Johannesburg data: {found_tiles}")
    else:
        print("\n⚠️  No Johannesburg tiles found in test set")
    
    return found_tiles


if __name__ == "__main__":
    # Try to find tiles
    result = find_tiles_alternative()
    
    if result is None:
        print("\nTrying alternative method...")
        found = download_and_check_tiles()
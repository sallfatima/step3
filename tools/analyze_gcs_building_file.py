#!/usr/bin/env python3
"""
Analyze building file directly from GCS using streaming
"""
import subprocess
import pandas as pd
import io


def analyze_gcs_file_streaming():
    """
    Analyze GCS file by streaming samples without full download
    """
    print("\n" + "="*60)
    print("ANALYZING GCS BUILDING FILE")
    print("="*60)
    
    gcs_path = "gs://lengo-geomapping/buildings_database/africa/south_africa/open_buildings_v3_polygons_ne_110m.csv.gz"
    
    print(f"File: {gcs_path}")
    
    # Get file info
    print("\n1. Getting file info...")
    info_cmd = ['gcloud', 'storage', 'ls', '-l', gcs_path]
    result = subprocess.run(info_cmd, capture_output=True, text=True)
    print(result.stdout)
    
    # Sample different parts of the file
    print("\n2. Sampling different parts of the file...")
    
    samples = []
    sample_positions = [
        ("Beginning", "0-1000000"),      # First 1MB
        ("Early", "10000000-11000000"),  # Around 10MB
        ("Middle", "3000000000-3001000000"),  # Around 3GB (middle)
        ("Late", "5000000000-5001000000"),   # Around 5GB
        ("End", "-1000000")              # Last 1MB
    ]
    
    for label, byte_range in sample_positions:
        print(f"\n--- Sampling {label} of file (bytes {byte_range}) ---")
        
        try:
            # Use gcloud storage cat with byte range
            if byte_range.startswith("-"):
                # For tail
                cmd = f"gcloud storage cat {gcs_path} | gzcat | tail -n 1000"
            else:
                # For specific byte range
                cmd = ['gcloud', 'storage', 'cat', f'--range={byte_range}', gcs_path]
            
            if isinstance(cmd, str):
                result = subprocess.run(cmd, shell=True, capture_output=True)
            else:
                # Pipe through gzcat to decompress
                proc1 = subprocess.Popen(cmd, stdout=subprocess.PIPE)
                proc2 = subprocess.Popen(['gzcat'], stdin=proc1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                result = proc2.communicate()[0]
            
            # Try to parse as CSV
            try:
                if isinstance(result, bytes):
                    text_data = result.decode('utf-8', errors='ignore')
                else:
                    text_data = result
                
                # Skip incomplete first line and parse
                lines = text_data.strip().split('\n')
                if len(lines) > 2:
                    # Create CSV from complete lines
                    csv_data = '\n'.join(lines[1:-1])  # Skip potentially incomplete first/last lines
                    
                    df = pd.read_csv(io.StringIO(csv_data), 
                                   names=['latitude', 'longitude', 'area_in_meters', 
                                         'confidence', 'geometry', 'full_plus_code'])
                    
                    if len(df) > 0:
                        lat_range = (df['latitude'].min(), df['latitude'].max())
                        lon_range = (df['longitude'].min(), df['longitude'].max())
                        
                        print(f"  Rows read: {len(df)}")
                        print(f"  Lat range: {lat_range[0]:.3f} to {lat_range[1]:.3f}")
                        print(f"  Lon range: {lon_range[0]:.3f} to {lon_range[1]:.3f}")
                        
                        # Identify region
                        if lat_range[1] < -20:
                            print(f"  ✓ SOUTH AFRICA DATA FOUND!")
                        elif 4 <= lat_range[0] <= 14:
                            print(f"  Region: West Africa")
                        elif -5 <= lat_range[0] <= 5:
                            print(f"  Region: Central/East Africa")
                        
                        # Check specifically for Johannesburg
                        jhb_data = df[
                            (df['latitude'] >= -26.5) & 
                            (df['latitude'] <= -25.8) &
                            (df['longitude'] >= 27.6) & 
                            (df['longitude'] <= 28.5)
                        ]
                        
                        if len(jhb_data) > 0:
                            print(f"  ✓✓ JOHANNESBURG BUILDINGS FOUND: {len(jhb_data)}")
                            print("\n  Sample Johannesburg buildings:")
                            print(jhb_data[['latitude', 'longitude', 'area_in_meters']].head())
                        
                        samples.append((label, df))
                
            except Exception as e:
                print(f"  Could not parse CSV: {e}")
                
        except Exception as e:
            print(f"  Could not sample: {e}")
    
    return samples


def find_south_africa_offset():
    """
    Try to find where South Africa data starts in the file
    """
    print("\n" + "="*60)
    print("SEARCHING FOR SOUTH AFRICA DATA OFFSET")
    print("="*60)
    
    gcs_path = "gs://lengo-geomapping/buildings_database/africa/south_africa/open_buildings_v3_polygons_ne_110m.csv.gz"
    
    print("\nThis will search for negative latitudes (Southern Hemisphere)...")
    
    # Use gcloud storage cat with grep to find first occurrence of negative latitude
    cmd = f'gcloud storage cat {gcs_path} | gzcat | grep -n "^-[0-9]" | head -10'
    
    print(f"\nSearching for lines starting with negative numbers...")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.stdout:
        print("\nFound potential South African data:")
        print(result.stdout)
        
        # Extract line numbers
        lines = result.stdout.strip().split('\n')
        for line in lines[:5]:
            parts = line.split(':', 1)
            if len(parts) == 2:
                line_num = parts[0]
                data = parts[1]
                print(f"\nLine {line_num}: {data[:100]}...")
    else:
        print("\n⚠️  No negative latitudes found in accessible part of file")
        print("The file might not contain South African data, or it might be compressed differently")
    
    # Alternative: Search for specific South African coordinates
    print("\n\nSearching for coordinates around Johannesburg (-26.xxx)...")
    cmd2 = f'gcloud storage cat {gcs_path} | gzcat | grep -m 10 "^-26\." | head -5'
    result2 = subprocess.run(cmd2, shell=True, capture_output=True, text=True)
    
    if result2.stdout:
        print("\nFound potential Johannesburg data:")
        print(result2.stdout)
    else:
        print("\n⚠️  No Johannesburg coordinates found")


def quick_country_check():
    """
    Quick check of which countries are in the file
    """
    print("\n" + "="*60)
    print("QUICK COUNTRY CHECK")
    print("="*60)
    
    gcs_path = "gs://lengo-geomapping/buildings_database/africa/south_africa/open_buildings_v3_polygons_ne_110m.csv.gz"
    
    # Download just first 100MB to analyze
    print("\nDownloading first 100MB for analysis...")
    cmd = ['gcloud', 'storage', 'cat', '--range=0-100000000', gcs_path]
    
    proc1 = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    proc2 = subprocess.Popen(['gzcat'], stdin=proc1.stdout, stdout=subprocess.PIPE)
    
    # Read into pandas
    df = pd.read_csv(proc2.stdout, nrows=1000000)
    
    print(f"\nAnalyzed {len(df):,} rows from beginning of file")
    print(f"\nCoordinate ranges:")
    print(f"  Latitude: {df['latitude'].min():.3f} to {df['latitude'].max():.3f}")
    print(f"  Longitude: {df['longitude'].min():.3f} to {df['longitude'].max():.3f}")
    
    # Identify countries
    print("\nIdentifying regions:")
    
    # West Africa
    wa_mask = (df['latitude'] > 4) & (df['latitude'] < 15) & (df['longitude'] > -5) & (df['longitude'] < 15)
    wa_count = wa_mask.sum()
    
    # South Africa
    sa_mask = (df['latitude'] < -22) & (df['latitude'] > -35) & (df['longitude'] > 16) & (df['longitude'] < 33)
    sa_count = sa_mask.sum()
    
    # East Africa
    ea_mask = (df['latitude'] > -10) & (df['latitude'] < 15) & (df['longitude'] > 30) & (df['longitude'] < 50)
    ea_count = ea_mask.sum()
    
    print(f"  West Africa: {wa_count:,} buildings ({wa_count/len(df)*100:.1f}%)")
    print(f"  South Africa: {sa_count:,} buildings ({sa_count/len(df)*100:.1f}%)")
    print(f"  East Africa: {ea_count:,} buildings ({ea_count/len(df)*100:.1f}%)")
    
    if sa_count == 0:
        print("\n⚠️  WARNING: No South African buildings found in first million rows!")
        print("The file might be sorted by country/region")
        print("South Africa data might be later in the file")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "find":
        find_south_africa_offset()
    elif len(sys.argv) > 1 and sys.argv[1] == "quick":
        quick_country_check()
    else:
        analyze_gcs_file_streaming()
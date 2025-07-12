import json
import os
from google.cloud import storage
from typing import Dict, Any
import requests
from logger import logger


def analyze_gcp_dataset(bucket_name: str, area_path: str, annotations_filename: str) -> Dict[str, Any]:
    """
    Analyze the size of a dataset stored in GCP
    
    Args:
        bucket_name: Name of the GCP bucket (e.g., "lengo-geomapping")
        area_path: Path to the area (e.g., "database/africa/south_africa/johannesburg/johannesburg_custom_ward_1")
        annotations_filename: Name of the annotations file
    
    Returns:
        Dictionary with dataset statistics
    """
    
    # Initialize storage client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    stats = {
        "images": {
            "count": 0,
            "total_size_mb": 0,
            "avg_size_mb": 0,
            "largest_size_mb": 0,
            "file_types": {}
        },
        "annotations": {
            "file_size_mb": 0,
            "num_images": 0,
            "num_annotations": 0,
            "num_categories": 0,
            "annotations_per_image": 0,
            "memory_estimate_mb": 0
        },
        "total_size_mb": 0,
        "estimated_memory_required_mb": 0
    }
    
    # 1. Analyze images
    logger.info(f"Analyzing images in {area_path}/images/...")
    images_prefix = f"{area_path}/images/"
    
    image_blobs = list(bucket.list_blobs(prefix=images_prefix))
    stats["images"]["count"] = len(image_blobs)
    
    for blob in image_blobs:
        size_mb = blob.size / (1024 * 1024)
        stats["images"]["total_size_mb"] += size_mb
        stats["images"]["largest_size_mb"] = max(stats["images"]["largest_size_mb"], size_mb)
        
        # Count file types
        ext = os.path.splitext(blob.name)[1].lower()
        stats["images"]["file_types"][ext] = stats["images"]["file_types"].get(ext, 0) + 1
    
    if stats["images"]["count"] > 0:
        stats["images"]["avg_size_mb"] = stats["images"]["total_size_mb"] / stats["images"]["count"]
    
    logger.info(f"Found {stats['images']['count']} images, total size: {stats['images']['total_size_mb']:.2f} MB")
    
    # 2. Analyze annotations
    logger.info(f"Analyzing annotations file: {annotations_filename}")
    annotations_path = f"{area_path}/annotations/{annotations_filename}"
    
    try:
        # Get annotation file blob
        ann_blob = bucket.blob(annotations_path)
        
        # Reload metadata to ensure we have the size
        if ann_blob.exists():
            ann_blob.reload()
            
            if ann_blob.size is not None:
                stats["annotations"]["file_size_mb"] = ann_blob.size / (1024 * 1024)
            else:
                logger.error(f"Could not get size for annotations file: {annotations_path}")
                # Try alternate method
                logger.info("Trying alternate method to get file info...")
                blobs = list(bucket.list_blobs(prefix=annotations_path))
                if blobs:
                    ann_blob = blobs[0]
                    stats["annotations"]["file_size_mb"] = ann_blob.size / (1024 * 1024) if ann_blob.size else 0
            
            # Download and analyze content (if not too large)
            if stats["annotations"]["file_size_mb"] < 100:  # Only download if < 100 MB
                logger.info("Downloading annotations to analyze content...")
                ann_content = ann_blob.download_as_text()
                ann_data = json.loads(ann_content)
                
                stats["annotations"]["num_images"] = len(ann_data.get("images", []))
                stats["annotations"]["num_annotations"] = len(ann_data.get("annotations", []))
                stats["annotations"]["num_categories"] = len(ann_data.get("categories", []))
                
                if stats["annotations"]["num_images"] > 0:
                    stats["annotations"]["annotations_per_image"] = (
                        stats["annotations"]["num_annotations"] / stats["annotations"]["num_images"]
                    )
                
                # Estimate memory usage
                # Each annotation in memory: ~1KB (bbox, class_id, metadata)
                # Each image metadata: ~0.5KB
                # Overhead: 3x for numpy arrays and data structures
                estimated_memory = (
                    (stats["annotations"]["num_annotations"] * 1 + 
                     stats["annotations"]["num_images"] * 0.5) * 3
                ) / 1024  # Convert to MB
                
                stats["annotations"]["memory_estimate_mb"] = estimated_memory
                
            else:
                logger.warning(f"Annotations file too large ({stats['annotations']['file_size_mb']:.2f} MB) to download for analysis")
                # Estimate based on file size
                # Rough estimate: JSON is verbose, actual data in memory is ~1/3 of JSON size
                stats["annotations"]["memory_estimate_mb"] = stats["annotations"]["file_size_mb"] * 3
        else:
            logger.error(f"Annotations file not found: {annotations_path}")
            
    except Exception as e:
        logger.error(f"Error analyzing annotations: {e}")
    
    # 3. Calculate totals
    stats["total_size_mb"] = stats["images"]["total_size_mb"] + stats["annotations"]["file_size_mb"]
    
    # Estimate total memory required
    # Dataset loading: annotations memory + image paths + overhead
    stats["estimated_memory_required_mb"] = (
        stats["annotations"]["memory_estimate_mb"] + 
        stats["images"]["count"] * 0.1 +  # Image paths and metadata
        1024  # 1GB overhead for Python/libraries
    )
    
    return stats


def print_analysis_report(stats: Dict[str, Any]) -> None:
    """Print a formatted analysis report"""
    
    print("\n" + "="*60)
    print("DATASET SIZE ANALYSIS REPORT")
    print("="*60)
    
    print("\n📸 IMAGES:")
    print(f"  - Count: {stats['images']['count']:,}")
    print(f"  - Total Size: {stats['images']['total_size_mb']:.2f} MB")
    print(f"  - Average Size: {stats['images']['avg_size_mb']:.2f} MB")
    print(f"  - Largest Image: {stats['images']['largest_size_mb']:.2f} MB")
    print(f"  - File Types: {stats['images']['file_types']}")
    
    print("\n📋 ANNOTATIONS:")
    print(f"  - File Size: {stats['annotations']['file_size_mb']:.2f} MB")
    print(f"  - Number of Images: {stats['annotations']['num_images']:,}")
    print(f"  - Number of Annotations: {stats['annotations']['num_annotations']:,}")
    print(f"  - Categories: {stats['annotations']['num_categories']}")
    print(f"  - Avg Annotations per Image: {stats['annotations']['annotations_per_image']:.1f}")
    print(f"  - Estimated Memory Usage: {stats['annotations']['memory_estimate_mb']:.2f} MB")
    
    print("\n💾 TOTALS:")
    print(f"  - Total Dataset Size: {stats['total_size_mb']:.2f} MB")
    print(f"  - Estimated Memory Required: {stats['estimated_memory_required_mb']:.2f} MB")
    
    print("\n💡 RECOMMENDATIONS:")
    if stats['estimated_memory_required_mb'] > 8192:
        print("  ⚠️  Dataset requires > 8GB RAM - Use batch processing!")
    if stats['estimated_memory_required_mb'] > 16384:
        print("  ⚠️  Dataset requires > 16GB RAM - Consider progressive loading!")
    if stats['annotations']['file_size_mb'] > 100:
        print("  ⚠️  Large annotations file - Consider streaming JSON parsing!")
    if stats['annotations']['num_annotations'] > 1000000:
        print("  ⚠️  Over 1M annotations - Use geographic chunking!")
    
    print("\n" + "="*60)


def main():
    """Example usage"""
    
    # Your dataset parameters
    bucket_name = "lengo-geomapping"
    area_path = "database/africa/south_africa/johannesburg/johannesburg_custom_ward_1"
    annotations_filename = "annotations_ward_1_no_duplicates_image.json"
    
    # Analyze
    logger.info("Starting dataset analysis...")
    stats = analyze_gcp_dataset(bucket_name, area_path, annotations_filename)
    
    # Print report
    print_analysis_report(stats)
    
    # Save detailed stats to file
    with open("dataset_analysis.json", "w") as f:
        json.dump(stats, f, indent=2)
    
    logger.info("Analysis complete! Results saved to dataset_analysis.json")


# Quick analysis function to add to your main code
def quick_check_dataset_size(bucket_name: str, annotations_blob_path: str) -> None:
    """Quick check to log dataset size before processing"""
    
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(annotations_blob_path)
        
        if blob.exists():
            size_mb = blob.size / (1024 * 1024)
            logger.info(f"Annotations file size: {size_mb:.2f} MB")
            
            if size_mb > 50:
                logger.warning(f"Large annotations file detected ({size_mb:.2f} MB)!")
                logger.warning("Consider using batch processing or progressive loading")
                
                # Quick estimate of memory needed
                estimated_memory_gb = (size_mb * 3) / 1024
                logger.info(f"Estimated memory required: {estimated_memory_gb:.1f} GB")
        else:
            logger.error(f"Annotations file not found: {annotations_blob_path}")
            
    except Exception as e:
        logger.error(f"Error checking dataset size: {e}")


if __name__ == "__main__":
    main()
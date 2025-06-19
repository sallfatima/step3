import base64
import concurrent.futures
import hashlib
import hmac
import json
import os
import urllib.parse as urlparse
from concurrent.futures import ThreadPoolExecutor
from typing import List

import cv2
import geopandas as gpd
import numpy as np
import shapefile
from config_model import SetupConfig
from google.cloud import storage
from google.cloud.storage import Bucket
from logger import logger
from shapely.geometry import shape


def get_signature(input_url: str = None, secret: str = None) -> str:
    """
    Sign a request URL with a URL signing secret.
    Usage:
      from urlsigner import sign_url
      signed_url = sign_url(input_url=my_url, secret=SECRET)
    Args:
        input_url: The URL to sign
        secret: Your URL signing secret

    Returns:
        The signed request URL

    """
    url = urlparse.urlparse(input_url)

    # We only need to sign the path+query part of the string
    url_to_sign = f"{url.path}?{url.query}"

    # Decode the private key into its binary format
    # We need to decode the URL-encoded private key
    decoded_key = base64.urlsafe_b64decode(secret)

    # Create a signature using the private key and the URL-encoded
    # string using HMAC SHA1. This signature will be binary.
    s = hmac.new(decoded_key, str.encode(url_to_sign), hashlib.sha1)

    # Encode the binary signature into base64 for use within a URL
    encoded_signature = base64.urlsafe_b64encode(s.digest())

    # Return signature
    return encoded_signature.decode()


def get_gcs_signed_url(
    cfg: SetupConfig, blob_name: str, expiration_sec: int = 604800
) -> str:
    """Generate a (signed) URL for a GCS object."""

    # Get bucket with credentials
    bucket = get_bucket(
        cfg.bucket_name,
        cfg.project_id,
        key_path=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
    )

    # Get blob
    blob = bucket.get_blob(blob_name)

    url = blob.generate_signed_url(
        version="v4", expiration=expiration_sec, method="GET"
    )
    return url


def get_bucket(bucket_name: str, project_id: str, key_path: str = None) -> Bucket:
    """
    Retrieves a GCP bucket
    Args:
        bucket_name: bucket name from GCP
        project_id: project id from GCP
        key_path: path to JSON file with Google credentials for a service account

    Returns:
        Bucket instance
    """

    # Create a client to interact with Google Cloud Storage
    if key_path:
        client = storage.Client(project=project_id).from_service_account_json(key_path)
    else:
        client = storage.Client(project=project_id)

    # Retrieve the image and save it locally
    bucket = client.get_bucket(bucket_name)

    return bucket


def copy_blob(
    source_bucket: Bucket,
    blob_name: str,
    destination_bucket: Bucket,
    destination_blob_name: str,
) -> None:
    """Copies a blob from one bucket to another with a new name."""

    source_blob = source_bucket.blob(blob_name)

    # Perform the copy
    _ = source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)

    logger.info(
        f"Copy -- Blob {source_blob.name} in bucket {source_bucket.name} copied to bucket {destination_bucket.name}."
    )


def set_folder_public(bucket: Bucket, folder_path: str):
    """Set a folder (prefix) and its objects to be publicly accessible."""

    # Set the ACL for the folder and its objects to be publicly accessible
    blobs = bucket.list_blobs(prefix=folder_path)
    for blob in blobs:
        blob.acl.all().grant_read()  # Grant read access to all users
        blob.acl.save()

    logger.info(f"Access -- Public access to all objects under {folder_path}.")


def set_folder_private(bucket: Bucket, folder_path: str) -> None:
    """Set a folder (prefix) and its objects to be private."""

    # Set the ACL for the folder and its objects to be private
    blobs = bucket.list_blobs(prefix=folder_path)
    for blob in blobs:
        blob.acl.all().revoke_read()  # Revoke read access for all users
        blob.acl.save()

    logger.info(f"Access -- Private access to all objects under {folder_path}.")


def upload_to_gcs(
    bucket: Bucket,
    source: str,
    destination_blob_name: str,
    content_type: str = "text/plain",
) -> None:
    """Uploads a file to a Google Cloud Storage bucket."""

    # Create a new blob
    blob = bucket.blob(destination_blob_name)

    # Upload to cloud
    blob.upload_from_string(source, content_type=content_type)


def upload_json_to_gcs(
    bucket: Bucket, destination_blob_name: str, data_dict: dict
) -> None:
    """Uploads a JSON file, to a Google Cloud Storage bucket"""

    # Build the json string
    json_data = json.dumps(data_dict)

    # Upload to cloud
    upload_to_gcs(
        bucket, json_data, destination_blob_name, content_type="application/json"
    )


def read_json_gcs(bucket: Bucket, source_file: str) -> dict:
    """Reads a JSON file from a Google Cloud Storage bucket."""

    # Create a new blob and upload the file's content.
    blob = bucket.blob(source_file)

    # Download the content of the blob as a string
    json_content = blob.download_as_string()

    # Parse the JSON content
    json_data = json.loads(json_content)

    return json_data


def get_names_gcs(bucket: Bucket, prefix: str) -> List[str]:
    """Reads all file names from a Google Cloud Storage bucket + prefix."""

    blobs = bucket.list_blobs(prefix=prefix)

    object_names = [blob.name for blob in blobs]
    return object_names


def clean_intermediate_files(cfg: SetupConfig, bucket: Bucket) -> None:
    """Cleans intermediate files used to build and merge graphs"""

    # Check for existence of merged OSM map and SV map
    merged_sv_path = f"{cfg.area.data_path}/{cfg.sv_name}_merged.json"
    merged_sv_map_exists = bucket.blob(merged_sv_path).exists()

    merged_osm_path = f"{cfg.area.data_path}/{cfg.osm_name}_merged.json"
    merged_osm_map_exists = bucket.blob(merged_osm_path).exists()

    assert merged_sv_map_exists, logger.error(
        f"Merge -- ERROR: SV map was not created!"
    )
    assert merged_osm_map_exists, logger.error(
        f"Merge -- ERROR: Merged OSM map was not created!"
    )

    logger.info(f"Merge -- Deleting intermediate files...")

    # List all blobs with the specified prefix
    blobs = bucket.list_blobs(prefix=cfg.area.output_path)

    # Function to delete a single blob
    def delete_blob(blob):
        blob.delete()
        logger.info(f"Deleted blob: {blob.name}")

    # Use ThreadPoolExecutor to delete blobs in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(delete_blob, blob) for blob in blobs]
        concurrent.futures.wait(futures)

    logger.info(f"Merge -- Intermediate files were deleted!")


def read_shapefiles(polygons_base_path: str, bucket: Bucket) -> gpd.GeoDataFrame:
    """Reads shapefiles from GCS, into a GeoDataFrame"""

    # Define paths
    shp_blob = bucket.blob(f"{polygons_base_path}.shp")
    shx_blob = bucket.blob(f"{polygons_base_path}.shx")
    dbf_blob = bucket.blob(f"{polygons_base_path}.dbf")

    with shp_blob.open(mode="rb") as shp, shx_blob.open(
        mode="rb"
    ) as shx, dbf_blob.open(mode="rb") as dbf:
        r = shapefile.Reader(shp=shp, dbf=dbf, shx=shx)

        # Extract shapes and records
        shapes = r.shapes()
        records = r.records()

    geometries = []
    attributes = []

    # Builds GeoDataFrame records
    for s, r in zip(shapes, records):
        if len(s.points) > 0:
            # Add only if geometry is not None
            geom = shape(s.__geo_interface__)
            geometries.append(geom)
            attributes.append(r.as_dict())

    # Create dataframe
    gdf = gpd.GeoDataFrame(attributes, geometry=geometries).set_crs("epsg:4326")

    return gdf


def download_blob(bucket, blob_name, destination_folder):
    """
    Download a single blob from a GCS bucket to a local folder.
    """
    blob = bucket.blob(blob_name)
    destination_path = os.path.join(destination_folder, os.path.basename(blob_name))
    logger.info(f"Downloading {blob_name} to {destination_path}...")
    blob.download_to_filename(destination_path)


def download_images(
    bucket, blob_names, destination_folder: str = "./temp_dataset", max_threads: int = 4
) -> None:
    """
    Download specific image blobs from a GCS bucket to a local folder using multiple threads.
    """
    os.makedirs(destination_folder, exist_ok=True)

    # Use ThreadPoolExecutor for parallel downloads
    with ThreadPoolExecutor(max_threads) as executor:
        # Submit tasks for each blob to be downloaded
        futures = [
            executor.submit(download_blob, bucket, blob_name, destination_folder)
            for blob_name in blob_names
        ]
        # Wait for all threads to complete
        for future in futures:
            try:
                future.result()  # Raise exceptions if any occurred in threads
            except Exception as e:
                logger.error(f"Error downloading blob: {e}")


def download_json_annotations(
    bucket: Bucket, blob_name: str, destination_path: str
) -> None:
    """
    Download specific image blobs from a GCS bucket to a local folder.
    """
    # Get the blob
    blob = bucket.blob(blob_name)

    # Download annotation file
    logger.info(f"Downloading {blob_name} to {destination_path}...")
    blob.download_to_filename(destination_path)
    logger.info(f"Downloaded {blob_name} successfully.")


def download_json_gcs(bucket: Bucket, blob_name: str, temp_save_path: str) -> None:
    """Download a JSON file from Google Cloud Storage and save it to a local file path."""

    # Access the blob (file) in the bucket
    blob = bucket.blob(blob_name)

    # Download the blob as bytes
    json_data = blob.download_as_bytes()

    # Save the bytes to a local file
    with open(temp_save_path, "wb") as file:
        file.write(json_data)


def read_image_from_gcs_opencv(bucket: Bucket, blob_name: str):
    """Read an image from Google Cloud Storage and return a cv2 image object."""

    # Access the blob (file) in the bucket
    blob = bucket.blob(blob_name)

    # Download the blob as bytes
    image_data = blob.download_as_bytes()

    # Convert bytes to numpy array
    np_array = np.frombuffer(image_data, np.uint8)

    # Decode image from numpy array
    image = cv2.imdecode(np_array, cv2.IMREAD_COLOR)

    return image

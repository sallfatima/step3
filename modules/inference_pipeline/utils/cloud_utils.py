import json

import cv2
import geopandas as gpd
import numpy as np
import shapefile
from google.cloud import storage
from google.cloud.storage import Bucket
from shapely.geometry import shape


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


def download_json_gcs(bucket: Bucket, blob_name: str, temp_save_path: str) -> None:
    """Download a JSON file from Google Cloud Storage and save it to a local file path."""

    # Access the blob (file) in the bucket
    blob = bucket.blob(blob_name)

    # Download the blob as bytes
    json_data = blob.download_as_bytes()

    # Save the bytes to a local file
    with open(temp_save_path, "wb") as file:
        file.write(json_data)


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

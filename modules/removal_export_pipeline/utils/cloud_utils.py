import io
import json
import math
import time

import cv2
import geopandas as gpd
import numpy as np
import pandas as pd
import shapefile
from google.api_core.exceptions import RetryError
from google.cloud import storage
from google.cloud.storage import Bucket
from PIL import Image
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


def upload_array_as_jpg(
    bucket: storage.Bucket,
    destination_blob_name: str,
    numpy_array,
    max_retries: int = 5,
    initial_delay: float = 1.0,
    base_timeout: int = 300,  # Base timeout of 5 minutes
    chunk_size: int = None,  # None for automatic, or set custom chunk size
) -> None:
    """
    Uploads a NumPy array as a JPEG image to Google Cloud Storage with production-grade resilience.

    Args:
        bucket: GCS Bucket object
        destination_blob_name: Destination path in GCS
        numpy_array: NumPy array to upload as JPEG
        max_retries: Maximum number of retry attempts (default: 5)
        initial_delay: Initial delay between retries in seconds (default: 1.0)
        base_timeout: Base timeout in seconds (default: 300)
        chunk_size: Custom chunk size in bytes for large files (default: None)
    """

    file_size_mb = numpy_array.nbytes / (1024 * 1024)
    timeout = max(base_timeout, int(math.ceil(file_size_mb * 60)))
    retry_delay = initial_delay

    for attempt in range(max_retries + 1):
        try:
            print(
                f"Attempt {attempt + 1}/{max_retries + 1}: Uploading {file_size_mb:.2f}MB file to {destination_blob_name} with {timeout}s timeout"
            )

            image = Image.fromarray(numpy_array.astype("uint8"))
            byte_stream = io.BytesIO()
            image.save(byte_stream, format="JPEG")
            byte_stream.seek(0)

            blob = bucket.blob(destination_blob_name)
            if chunk_size or numpy_array.nbytes > 10 * 1024 * 1024:  # >10MB
                effective_chunk_size = chunk_size or 10 * 1024 * 1024  # 10MB default
                blob.chunk_size = effective_chunk_size
                print(
                    f"Using chunked upload with {effective_chunk_size/(1024*1024):.1f}MB chunks"
                )

            blob.upload_from_file(
                byte_stream,
                content_type="image/jpeg",
                timeout=timeout,
                num_retries=max_retries,
                if_generation_match=None,  # Optional: add generation match for versioning
                checksum="md5",  # Enable checksum verification
            )

            print(f"Successfully uploaded to {destination_blob_name}")
            return

        except Exception as e:
            if attempt == max_retries:
                error_msg = (
                    f"Max retries ({max_retries}) reached. Failed to upload {destination_blob_name}. "
                    f"Last error: {str(e)}"
                )
                print(error_msg)
                raise RuntimeError(error_msg) from e

            print(f"Attempt {attempt + 1} failed with error: {str(e)}")
            print(f"Waiting {retry_delay:.1f} seconds before retry...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)  # Exponential backoff with 60s cap

        finally:
            if "byte_stream" in locals():
                byte_stream.close()
            if "image" in locals():
                image.close()


# def upload_array_as_jpg(
#     bucket: Bucket, destination_blob_name: str, numpy_array
# ) -> None:
#     # Step 1: Convert the NumPy array to a PIL Image
#     image = Image.fromarray(numpy_array.astype("uint8"))

#     # Step 2: Save the image to a byte stream in JPEG format
#     byte_stream = io.BytesIO()
#     image.save(byte_stream, format="JPEG")
#     byte_stream.seek(0)  # Go back to the start of the byte stream

#     # Step 3: Upload the byte stream to GCS
#     blob = bucket.blob(destination_blob_name)
#     blob.upload_from_file(byte_stream, content_type="image/jpeg")


def upload_dataframe_to_gcs(
    bucket: Bucket, df: pd.DataFrame, destination_blob_name: str, export_format: str
) -> None:
    """Uploads a dataframe in Excel format, to the bucket."""
    blob = bucket.blob(destination_blob_name)

    if export_format == "excel":
        with io.BytesIO() as output:
            writer = pd.ExcelWriter(output, engine="xlsxwriter")
            df.to_excel(writer, index=False)
            writer.close()
            output.seek(0)
            blob.upload_from_file(
                output,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    elif export_format == "csv":
        with io.StringIO() as output_buffer:
            df.to_csv(
                output_buffer, index=False
            )  # Exclude index by setting index=False
            output_buffer.seek(0)
            blob.upload_from_file(output_buffer, content_type="text/csv")
    else:
        raise ValueError("The format is not supported. PLease use excel or csv.")


def upload_to_gcs(
    bucket: Bucket,
    source: str,
    destination_blob_name: str,
    content_type: str = "text/plain",
) -> None:
    """
    Uploads a string to a Google Cloud Storage bucket.
    """
    try:
        # Create a new blob
        blob = bucket.blob(destination_blob_name)

        # Retry logic for transient issues
        retries = 3
        for attempt in range(retries):
            try:
                print(f"Attempt {attempt + 1}: Uploading to {destination_blob_name}")
                # Upload to cloud
                blob.upload_from_string(
                    source, content_type=content_type, timeout=1200, num_retries=10
                )
                print(f"Successfully uploaded to {destination_blob_name}")
                break
            except RetryError as retry_err:
                print(f"Retry {attempt + 1} failed: {retry_err}")
                if attempt == retries - 1:
                    raise
    except Exception as e:
        print(f"Error during upload: {e}")
        raise


def upload_json_to_gcs(
    bucket: Bucket, destination_blob_name: str, data_dict: dict
) -> None:
    """
    Uploads a JSON file to a Google Cloud Storage bucket.
    """
    try:
        # Build the JSON string
        json_data = json.dumps(data_dict)

        # Upload to GCS
        upload_to_gcs(
            bucket,
            json_data,
            destination_blob_name,
            content_type="application/json",
        )
    except Exception as e:
        print(f"Error uploading JSON to GCS: {e}")
        raise


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


def read_json_gcs(bucket: Bucket, source_file: str) -> dict:
    """Reads a JSON file from a Google Cloud Storage bucket."""

    # Create a new blob and upload the file's content.
    blob = bucket.blob(source_file)

    # Download the content of the blob as a string
    json_content = blob.download_as_string()

    # Parse the JSON content
    json_data = json.loads(json_content)

    return json_data

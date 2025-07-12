import json
import os
import shutil
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import List

import numpy as np
import requests
from cloud_utils import (
    download_images,
    get_gcs_signed_url,
    get_names_gcs,
    upload_json_to_gcs,
)
from config_model import SetupConfig
from dataset_utils import CloudDetectionDataset, DetectionDataset
from google.cloud.storage import Bucket
from logger import logger
from roboflow import Roboflow
from roboflow.adapters.rfapi import AnnotationSaveError as AlreadyExistingError

# Define pause event and set it
pause_event = Event()
pause_event.set()


def upload_image_roboflow_run(
    cfg: SetupConfig,
    index: int,
    blob_name: str,
    nr_blobs: int,
    split: str = "train",
) -> None:
    """Uploads image to Roboflow"""

    # Get config parameters
    upload_cfg = cfg.features.upload_for_annotation
    presigned_url = get_gcs_signed_url(cfg, blob_name)

    api_key = upload_cfg.roboflow_api_key
    project_name = upload_cfg.roboflow_project_name

    # Get base URL
    base_url = "https://api.roboflow.com"

    # Get image name
    img_name = presigned_url.split("/")[-1]

    # Build upload URL
    upload_url = "".join(
        [
            f"{base_url}/dataset/{project_name}/upload",
            "?api_key=" + api_key,
            "&name=" + img_name,
            "&split=" + split,
            "&image=" + urllib.parse.quote_plus(presigned_url),
        ]
    )

    # Check response code
    while True:
        pause_event.wait()

        try:
            response = requests.post(upload_url)
            response.raise_for_status()
            logger.info(f"Upload -- to Roboflow, progress is {index}/{nr_blobs - 1}")
            break
        except Exception as e:
            pause_event.clear()
            logger.error(f"Upload -- Error for {img_name}: {e}")
            time.sleep(60)
            pause_event.set()


def upload_image(
    project, local_image_path, annotations_destination_path, cfg, split, tag_list
):
    """
    Upload a single image to Roboflow.
    """
    try:
        result = project.single_upload(
            image_path=local_image_path,
            annotation_path=annotations_destination_path,
            batch_name=f"Code upload - {cfg.area.name}",
            split=split,
            num_retry_uploads=2,
            is_prediction=True,
            tag_names=tag_list,
        )
        print(result)
    except AlreadyExistingError as e:
        logger.warning(f"Image already exists: {e}")


def get_indexes(classes, query) -> np.ndarray:
    index_map = {element.lower(): i for i, element in enumerate(classes)}
    return np.array(
        [index_map[item.lower()] for item in query if item.lower() in index_map]
    )


def filter_dataset(
    dataset: CloudDetectionDataset, upload_classes_name: List[str]
) -> CloudDetectionDataset:
    """
    Filters the dataset to include only the upload_classes_name to be uploaded
    Args:
        dataset: original dataset instance
        upload_classes_name: list of class names to be uploaded

    Returns:
        new dataset
    """
    marked_for_deletion = []
    for image_path, image, annotation in dataset:
        annotation = annotation[
            np.isin(
                annotation.class_id, get_indexes(dataset.classes, upload_classes_name)
            )
        ]
        # Keep only annotations of the required classes
        dataset.annotations[image_path] = annotation

        # Mark images with no bounding boxes or no classes of interest
        if len(annotation) == 0:
            marked_for_deletion.append(image_path)

    # Delete those images from the dataset to be uploaded (and their annotations)
    for image_path in marked_for_deletion:
        dataset.image_paths.remove(image_path)
        del dataset.annotations[image_path]

    return dataset


def upload_for_annotation(cfg: SetupConfig, bucket: Bucket) -> None:
    """Uploads a dataset to Roboflow or VertexAI, depending on the config file"""

    # Parse config parameters
    upload_cfg = cfg.features.upload_for_annotation
    annotations_source = upload_cfg.annotations_database_source
    annotations_filename = upload_cfg.annotations_filename
    filtered_images = upload_cfg.filtered_images
    split = upload_cfg.split
    tag_list = upload_cfg.tag_list
    upload_classes_name = upload_cfg.upload_classes_name

    # Initialize Roboflow client
    rf = Roboflow(api_key=upload_cfg.roboflow_api_key)
    workspace = rf.workspace()
    project = workspace.project(upload_cfg.roboflow_project_name)

    filtered_annotation_path = ""
    temp_dataset_path = ""
    if annotations_source is not None:
        # Take images from an annotation file in the area
        annotations_path = (
            f"{annotations_source}/{cfg.area.name}"
            f"/annotations/{annotations_filename}"
        )
        # Check if annotation file exists
        annotation_exists = bucket.blob(annotations_path).exists()

        if annotation_exists:
            # Create detection dataset and obtain all blobs
            dataset = CloudDetectionDataset.from_coco(
                images_directory_path=cfg.area.images_path,
                annotations_path=annotations_path,
                bucket=bucket,
            )

            # Filter dataset based on classes
            dataset = filter_dataset(dataset, upload_classes_name)

            # Get all image paths left
            available_blobs = dataset.image_paths

            # Set temporary local dataset path
            temp_dataset_path = "./temp_dataset/"
            os.makedirs(temp_dataset_path, exist_ok=True)

            # Export back to coco dataset
            filtered_annotation_path = os.path.join(
                temp_dataset_path, "annotations_filtered.json"
            )
            dataset.as_coco(filtered_annotation_path)

        else:
            error_text = (
                f"Annotation file does not exists on the cloud: {annotations_path}"
            )
            logger.error(error_text)
            raise ValueError(error_text)

    else:
        # Take all images from the images folder corresponding to the area
        available_blobs = sorted(get_names_gcs(bucket, prefix=cfg.area.images_path))

    # Filter list of blobs based on filtered images
    if filtered_images is not None and len(filtered_images) > 0:
        available_blobs = [
            blob
            for blob in available_blobs
            if os.path.basename(blob) in filtered_images
        ]

    # If there are no annotations, upload only the images
    if annotations_source is None:
        # Get local annotations path
        logger.info(f"Upload -- started uploading to Roboflow...")

        max_chunk_size = upload_cfg.max_chunk_size
        for i in range(0, len(available_blobs), max_chunk_size):
            cur_blobs_name = available_blobs[i : i + max_chunk_size]
            cur_indexes = list(range(i, i + max_chunk_size))
            retrieve_threads = []

            for cur_blob_name, index in zip(cur_blobs_name, cur_indexes):
                thread = threading.Thread(
                    target=upload_image_roboflow_run,
                    args=(cfg, index, cur_blob_name, len(available_blobs), split),
                    daemon=True,
                )
                retrieve_threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in retrieve_threads:
                thread.join()
    else:
        # Download images
        image_destination_folder = os.path.join(temp_dataset_path, split)
        download_images(
            bucket, available_blobs, destination_folder=image_destination_folder
        )

        # Get images to be uploaded
        images_to_upload = [
            os.path.join(image_destination_folder, image)
            for image in os.listdir(image_destination_folder)
            if image.endswith(".jpg")
        ]

        logger.info(f"Upload -- started uploading to Roboflow...")
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(
                    upload_image,
                    project,
                    image_path,
                    filtered_annotation_path,
                    cfg,
                    split,
                    tag_list,
                )
                for image_path in images_to_upload
            ]
            for future in futures:
                try:
                    future.result()  # Raise exceptions if any occurred in threads
                except Exception as e:
                    logger.error(f"Error during upload: {e}")

        # Remove temporary folder
        shutil.rmtree(temp_dataset_path)

def upload_from_annotation(cfg: SetupConfig, bucket: Bucket) -> None:
    """(Merges) and uploads local annotation files, from Roboflow, to GCS"""
    import tempfile
    import shutil
    import json
    import uuid

    # Generate unique execution ID for debugging
    execution_id = str(uuid.uuid4())[:8]
    logger.info(f"[EXEC-{execution_id}] Starting upload_from_annotation")

    # Get config params
    features_cfg = cfg.features
    local_sources = features_cfg.upload_from_annotation.local_sources
    target_annotations_filename = (
        features_cfg.upload_from_annotation.annotations_filename
    )
    area_annotations_path = cfg.area.annotations_path

    # Set Roboflow default annotation filename
    default_annotation_filename = "_annotations.coco.json"

    logger.info(f"[EXEC-{execution_id}] Processing {len(local_sources)} annotation source(s)")
    logger.info(f"[EXEC-{execution_id}] Target filename: {target_annotations_filename}")
    logger.info(f"[EXEC-{execution_id}] Area annotations path: {area_annotations_path}")

    # 🚀 OPTIMIZATION: Single source - direct copy (fast path)
    if len(local_sources) == 1:
        logger.info(f"[EXEC-{execution_id}] 🚀 FAST PATH: Single source detected")
        local_source = local_sources[0]
        
        # Handle GCS source with optimized download
        if not local_source.startswith('/') and not os.path.exists(local_source):
            logger.info(f"[EXEC-{execution_id}] Downloading annotation file from GCS: {local_source}")
            
            # Download only the annotation file (not all images)
            annotation_blob_path = f"{local_source}/{default_annotation_filename}"
            annotation_blob = bucket.blob(annotation_blob_path)
            
            if not annotation_blob.exists():
                logger.error(f"[EXEC-{execution_id}] Annotation file not found: {annotation_blob_path}")
                raise FileNotFoundError(f"Annotation file not found in GCS: {annotation_blob_path}")
            
            # Use temp file with execution ID to avoid conflicts
            temp_annotation_file = f"temp_{execution_id}_{default_annotation_filename}"
            logger.info(f"[EXEC-{execution_id}] Downloading to: {temp_annotation_file}")
            annotation_blob.download_to_filename(temp_annotation_file)
            
            # Read and process
            with open(temp_annotation_file, "r") as source_file:
                data_dict = json.load(source_file)
            
            # Cleanup temp file immediately
            os.remove(temp_annotation_file)
            logger.info(f"[EXEC-{execution_id}] Temp file cleaned up")
            
        else:
            # Local source
            logger.info(f"[EXEC-{execution_id}] Using local path: {local_source}")
            annotations_path = os.path.join(local_source, default_annotation_filename)
            
            if not os.path.exists(annotations_path):
                logger.error(f"[EXEC-{execution_id}] Local annotation file not found: {annotations_path}")
                raise FileNotFoundError(f"Annotation file not found: {annotations_path}")
            
            with open(annotations_path, "r") as source_file:
                data_dict = json.load(source_file)

        # Add enhanced metadata
        if "info" not in data_dict:
            data_dict["info"] = {}
        data_dict["info"]["description"] = f"Annotations for {cfg.area.name}"
        data_dict["info"]["contributor"] = f"geo-mapping upload_from_annotation [EXEC-{execution_id}]"
        data_dict["info"]["processing_mode"] = "fast_path_single_source"
        
        logger.info(f"[EXEC-{execution_id}] ✅ Fast path completed - {len(data_dict.get('images', []))} images, {len(data_dict.get('annotations', []))} annotations")
        
    else:
        # 🔧 ROBUST PATH: Multiple sources - use CloudDetectionDataset (robust but slower)
        logger.info(f"[EXEC-{execution_id}] 🔧 ROBUST PATH: Multiple sources detected ({len(local_sources)})")
        
        datasets = []
        temp_dirs_to_cleanup = []

        for idx, local_source in enumerate(local_sources):
            logger.info(f"[EXEC-{execution_id}] Processing source {idx+1}/{len(local_sources)}: {local_source}")
            
            # Handle GCS sources with full download
            if not local_source.startswith('/') and not os.path.exists(local_source):
                logger.info(f"[EXEC-{execution_id}] Downloading from GCS path: {local_source}")
                
                temp_dir = tempfile.mkdtemp(prefix=f"gcs_download_{execution_id}_")
                actual_local_source = os.path.join(temp_dir, "annotations")
                os.makedirs(actual_local_source, exist_ok=True)
                temp_dirs_to_cleanup.append(temp_dir)
                
                # Download all files for this source
                blobs = bucket.list_blobs(prefix=local_source)
                downloaded_files = 0
                
                for blob in blobs:
                    if blob.name.endswith('/'):  # Skip directory markers
                        continue
                        
                    relative_path = blob.name[len(local_source):].lstrip('/')
                    if not relative_path:
                        continue
                        
                    local_file_path = os.path.join(actual_local_source, relative_path)
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    
                    blob.download_to_filename(local_file_path)
                    downloaded_files += 1
                
                logger.info(f"[EXEC-{execution_id}] Downloaded {downloaded_files} files from source {idx+1}")
            else:
                actual_local_source = local_source
                logger.info(f"[EXEC-{execution_id}] Using local path: {actual_local_source}")

            annotations_path = os.path.join(actual_local_source, default_annotation_filename)
            
            if not os.path.exists(annotations_path):
                logger.error(f"[EXEC-{execution_id}] Annotation file not found: {annotations_path}")
                raise FileNotFoundError(f"Annotation file not found: {annotations_path}")

            # Use CloudDetectionDataset for robust processing
            logger.info(f"[EXEC-{execution_id}] Creating CloudDetectionDataset for source {idx+1}")
            dataset = CloudDetectionDataset.from_coco(
                images_directory_path=actual_local_source, 
                annotations_path=annotations_path,
                bucket=bucket
            )
            
            logger.info(f"[EXEC-{execution_id}] Source {idx+1}: {len(dataset.image_paths)} images, {len(dataset.classes)} classes: {dataset.classes}")
            datasets.append(dataset)

        # Merge datasets with CloudDetectionDataset robustness
        logger.info(f"[EXEC-{execution_id}] Merging {len(datasets)} datasets...")
        try:
            final_dataset = CloudDetectionDataset.merge(datasets, bucket)
            logger.info(f"[EXEC-{execution_id}] ✅ Merge successful: {len(final_dataset.image_paths)} total images")
        except ValueError as e:
            logger.error(f"[EXEC-{execution_id}] ❌ Merge failed: {e}")
            raise

        # Export to COCO format
        logger.info(f"[EXEC-{execution_id}] Exporting merged dataset to COCO format")
        final_dataset.as_coco(annotations_path=target_annotations_filename)
        
        # Read the merged annotation file
        with open(target_annotations_filename, "r") as json_file:
            data_dict = json.load(json_file)
            
        # Add enhanced metadata for multi-source
        if "info" not in data_dict:
            data_dict["info"] = {}
        data_dict["info"]["description"] = f"Merged annotations for {cfg.area.name}"
        data_dict["info"]["contributor"] = f"geo-mapping upload_from_annotation [EXEC-{execution_id}]"
        data_dict["info"]["processing_mode"] = f"robust_path_multi_source_{len(local_sources)}_sources"
        data_dict["info"]["source_count"] = len(local_sources)
        
        # Cleanup temporary directories
        for temp_dir in temp_dirs_to_cleanup:
            logger.info(f"[EXEC-{execution_id}] Cleaning up temp directory: {temp_dir}")
            shutil.rmtree(temp_dir)
            
        logger.info(f"[EXEC-{execution_id}] ✅ Robust path completed - {len(data_dict.get('images', []))} images, {len(data_dict.get('annotations', []))} annotations")

    # Write local target file (common for both paths)
    logger.info(f"[EXEC-{execution_id}] Writing local target file: {target_annotations_filename}")
    with open(target_annotations_filename, "w") as target_file:
        json.dump(data_dict, target_file, indent=2)

    # Upload to GCS (common final step)
    gsc_filepath = f"{area_annotations_path}/{target_annotations_filename}"
    logger.info(f"[EXEC-{execution_id}] Target GCS path: {gsc_filepath}")

    # Check for file existence
    if bucket.blob(gsc_filepath).exists():
        error_text = f"[EXEC-{execution_id}] The annotation file {target_annotations_filename} already exists, cannot override!"
        logger.error(error_text)
        raise ValueError(error_text)
    else:
        logger.info(f"[EXEC-{execution_id}] Uploading to GCS: {gsc_filepath}")
        upload_json_to_gcs(bucket, gsc_filepath, data_dict)
        logger.info(f"[EXEC-{execution_id}] ✅ Successfully uploaded to GCS")

    # Cleanup local file
    if os.path.exists(target_annotations_filename):
        os.remove(target_annotations_filename)
        logger.info(f"[EXEC-{execution_id}] Local file cleaned up")
    
    # Final summary
    total_images = len(data_dict.get('images', []))
    total_annotations = len(data_dict.get('annotations', []))
    processing_mode = data_dict.get('info', {}).get('processing_mode', 'unknown')
    
    logger.info(f"[EXEC-{execution_id}] 🎉 COMPLETED - Mode: {processing_mode}, Images: {total_images}, Annotations: {total_annotations}")
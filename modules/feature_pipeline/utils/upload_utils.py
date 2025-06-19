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

    # Get config params
    features_cfg = cfg.features
    classes = features_cfg.upload_from_annotation.common_classes
    local_sources = features_cfg.upload_from_annotation.local_sources
    target_annotations_filename = (
        features_cfg.upload_from_annotation.annotations_filename
    )
    area_annotations_path = cfg.area.annotations_path

    # Set Roboflow default annotation filename
    default_annotation_filename = "_annotations.coco.json"

    # Initialize datasets
    datasets = []

    for local_source in local_sources:
        annotations_path = os.path.join(local_source, default_annotation_filename)

        # Create supervision dataset
        dataset = DetectionDataset.from_coco(
            images_directory_path=local_source, annotations_path=annotations_path
        )
        dataset.classes = classes
        datasets.append(dataset)

    # Merge datasets
    final_dataset = DetectionDataset.merge(datasets)
    final_dataset.as_coco(annotations_path=target_annotations_filename)

    # Read annotation json
    with open(target_annotations_filename, "r") as json_file:
        data_dict = json.load(json_file)

    # Sert path in GCS
    gsc_filepath = f"{area_annotations_path}/{target_annotations_filename}"

    # Check for file existence
    if bucket.blob(gsc_filepath).exists():
        error_text = f"The annotation file with name {target_annotations_filename} already exists, cannot override!"
        logger.error(error_text)
        raise ValueError(error_text)
    else:
        # Upload to GCS
        upload_json_to_gcs(bucket, gsc_filepath, data_dict)

    # Remove local, merged, annotation file
    os.remove(target_annotations_filename)

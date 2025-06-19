import asyncio
import concurrent.futures
import io
import json
import os
import shutil
from typing import Dict, List

import cv2
import numpy as np
import supervision as sv
import tqdm
from cloud_utils import upload_json_to_gcs
from config_model import SetupConfig
from dataset_utils import CloudDetectionDataset
from google.cloud.storage import Bucket
from logger import logger
from mmdet.apis import inference_detector, init_detector
from PIL import Image
from shapely.geometry import Point, Polygon
from supervision.detection.core import Detections


def copy_config_file(src: str, dst: str):
    shutil.copy(src, dst)
    logger.info(f"Config file copied from {src} to {dst}")


async def download_image(bucket: Bucket, image_file: str):
    blob = bucket.blob(image_file)
    image_data = await asyncio.to_thread(blob.download_as_bytes)
    return image_data, image_file


def decode_image(image_data):
    image_array = np.frombuffer(image_data, dtype=np.uint8)
    image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
    return image


async def fetch_images(bucket: Bucket, image_files: List[str]):
    tasks = [download_image(bucket, image_file) for image_file in image_files]
    return await asyncio.gather(*tasks)


async def upload_to_gcs(blob, image_byte_array):
    await asyncio.to_thread(
        blob.upload_from_string, image_byte_array.getvalue(), content_type="image/png"
    )


def save_image(
    predicted_image,
    detections,
    bucket,
    gcs_path,
    annotate=False,
    bounding_box_annotator=None,
    label_annotator=None,
):

    if annotate:
        assert (
            bounding_box_annotator is not None and label_annotator is not None
        ), "You need to provide annotators because annotate is active"
        # Annotate the image with the bounding box
        predicted_image = bounding_box_annotator.annotate(predicted_image, detections)
        # Annotate the image with the class label
        predicted_image = label_annotator.annotate(predicted_image, detections)

    # Transform the image array to an Image
    predicted_image = predicted_image.astype(np.uint8)
    predicted_image = predicted_image[..., ::-1]
    predicted_image = Image.fromarray(predicted_image)

    # Save the image to buffer
    image_byte_array = io.BytesIO()
    predicted_image.save(image_byte_array, format="PNG")
    image_byte_array.seek(0)

    # Upload image to cloud
    blob = bucket.blob(gcs_path)
    asyncio.run(upload_to_gcs(blob, image_byte_array))


def create_upload_annotations(
    model,
    img_paths: List[str],
    annotations: Dict[str, Detections],
    bucket: Bucket,
    annotations_path: str,
    ann_filename: str = "annotations.json",
):

    # Create dataset from Detections
    dataset = CloudDetectionDataset(
        classes=model.cfg.classes,
        images=img_paths,
        annotations=annotations,
        bucket=bucket,
    )

    # Export in COCO format, locally
    dataset.as_coco(annotations_path=ann_filename)

    # Read annotation json
    with open(ann_filename, "r") as json_file:
        data_dict = json.load(json_file)

    # Upload to GCS
    gcs_filepath = f"{annotations_path}/{ann_filename}"
    upload_json_to_gcs(bucket, gcs_filepath, data_dict)

    # Remove unwanted files
    if os.path.exists("temp_annotations.json"):
        os.remove("temp_annotations.json")
    os.remove(ann_filename)


def process_detections(results):
    """
    Processes a list of detection results (one per class) and consolidates into a single Detections object.

    Args:
        results (list of np.ndarray): Each element is a NumPy array of shape (N, 5),
                                      where each row is [x1, y1, x2, y2, confidence].

    Returns:
        Detections: Consolidated detection object from supervision library.
    """
    all_xyxy = []
    all_confidence = []
    all_class_id = []

    # Iterate through each class's detections
    for class_id, class_detections in enumerate(results):
        if class_detections.size == 0:  # Skip empty arrays
            continue

        # Extract bounding boxes and confidence scores
        all_xyxy.append(class_detections[:, :4])  # Bounding box coordinates
        all_confidence.append(class_detections[:, 4])  # Confidence scores
        all_class_id.append(
            np.full((class_detections.shape[0],), class_id, dtype=np.int32)
        )  # Class IDs

    # Concatenate all detections
    xyxy_array = (
        np.vstack(all_xyxy) if len(all_xyxy) > 0 else np.empty((0, 4), dtype=np.float32)
    )
    confidence_array = (
        np.hstack(all_confidence)
        if len(all_confidence) > 0
        else np.empty((0,), dtype=np.float32)
    )
    class_id_array = (
        np.hstack(all_class_id)
        if len(all_class_id) > 0
        else np.empty((0,), dtype=np.int32)
    )
    return Detections(
        xyxy=xyxy_array, confidence=confidence_array, class_id=class_id_array
    )


def process_batch(
    image_index,
    batch_images,
    batch_files,
    result,
    bounding_box_annotator,
    label_annotator,
    agnostic_nms,
    agnostic_nms_thresh,
    bbox_conf_thresh,
    keep_only_cpgs,
    annotations,
    img_paths,
    store_predictions,
    prediction_path,
    bucket,
    active_sampling_cfg,
    sampled_annotations,
    sampled_img_paths,
):
    # Parse active sampling config
    enabled_active_sampling = active_sampling_cfg.enable
    active_sampling_confidence_interval = active_sampling_cfg.confidence_interval
    active_sampling_probability = active_sampling_cfg.probability
    active_sampling_selected_classes = active_sampling_cfg.selected_classes

    assert len(active_sampling_probability) == len(
        active_sampling_selected_classes
    ), "Probability and classes does not have the same length"

    # Perform detection on the batch
    for idx, image_filepath in enumerate(batch_files):
        img_paths.append(image_filepath)

        logger.info(f"Predicting image with index {image_index}...")

        # Transform detections from MMDetection format to supervision Detections
        # detections = sv.Detections.from_mmdetection(result[idx])
        # NOTE: Above line from supervision does not work for Co-DETR repo,
        # as the mmdetection version is too old to be compatible with current supervision version

        detections = process_detections(result[idx])

        # Active sampling
        if enabled_active_sampling:
            final_sampled_detections = []
            # Confidence thresholding
            sampled_detections_confidence = detections[
                (detections.confidence >= active_sampling_confidence_interval[0])
                & (detections.confidence <= active_sampling_confidence_interval[1])
            ]
            if len(sampled_detections_confidence) > 0:
                # Class selection
                sampled_detections_classes = sampled_detections_confidence[
                    np.isin(
                        sampled_detections_confidence.class_id,
                        np.array(active_sampling_selected_classes),
                    )
                ]

                if len(sampled_detections_classes) > 0:
                    # Probability selection
                    for class_id, probability in zip(
                        active_sampling_selected_classes, active_sampling_probability
                    ):
                        # Check detections for specific class
                        sampled_detections_class = sampled_detections_classes[
                            sampled_detections_classes.class_id == class_id
                        ]

                        if len(sampled_detections_class) > 0:
                            # Keep detections with probability (needs to be in [0,1) interval)
                            mask = np.random.rand(len(sampled_detections_class)) < (
                                probability / 100
                            )
                            sampled_detections_result = sampled_detections_class[mask]

                            if len(sampled_detections_result) > 0:
                                final_sampled_detections.append(
                                    sampled_detections_result
                                )
            # Combine detections
            if len(final_sampled_detections) > 0:
                final_sampled_detections = sv.Detections.merge(final_sampled_detections)
                sampled_annotations.update({image_filepath: final_sampled_detections})
                sampled_img_paths.append(image_filepath)

        # Apply class agnostic NMS
        if agnostic_nms:
            detections = detections.with_nms(
                threshold=agnostic_nms_thresh, class_agnostic=False
            )

        # Filter detections by confidence score
        detections = detections[detections.confidence > bbox_conf_thresh]

        # Keep only CPGs or not
        if keep_only_cpgs:
            detections = detections[detections.class_id == 0]

        # Update annotations dict
        annotations.update({image_filepath: detections})

        if store_predictions and len(detections) > 0:
            # Annotate and upload image to gcs
            gcs_path = f"{prediction_path}/{os.path.basename(image_filepath)}"
            save_image(
                batch_images[idx].copy(),
                detections,
                bucket,
                gcs_path,
                annotate=True,
                bounding_box_annotator=bounding_box_annotator,
                label_annotator=label_annotator,
            )


def predict(cfg: SetupConfig, bucket: Bucket):
    # Parse config parameters
    prediction_cfg = cfg.inference.predict
    active_sampling_cfg = prediction_cfg.active_sampling
    model_name = prediction_cfg.model_name
    config_name = prediction_cfg.config_name

    images_path = cfg.area.images_path
    annotations_path = cfg.area.annotations_path
    annotations_filename = cfg.annotations_filename
    prediction_path = cfg.area.predictions_path

    # Check for the existence of the current annotation file, not to be overwritten
    gcs_annotation_filepath = f"{annotations_path}/{annotations_filename}"
    if bucket.blob(gcs_annotation_filepath).exists():
        text_error = (
            f"The annotation file {annotations_filename} already exists for this area."
            f" Please change the annotation_filename in general/settings.yaml"
        )
        logger.error(text_error)
        raise ValueError(text_error)

    store_predictions = prediction_cfg.store_predictions
    batch_size = prediction_cfg.batch_size
    agnostic_nms = prediction_cfg.agnostic_nms
    agnostic_nms_thresh = prediction_cfg.agnostic_nms_thresh
    bbox_conf_thresh = prediction_cfg.bbox_conf_thresh
    keep_only_cpgs = prediction_cfg.keep_only_cpgs

    # Define destination path, in mmdetection project, to store the model config file
    config_destintation_path = f"Co-DETR/projects/configs/co_dino_vit/{config_name}"

    # Define source path for the model config file
    config_source_path = f"network_configs/{config_name}"

    # Copy config file
    copy_config_file(config_source_path, config_destintation_path)

    # Obtain country name
    country_path = "/".join(cfg.area.name.split("/")[:2])

    # Obtain best model for the country
    gcs_model_path = f"{cfg.models_database_path}/{country_path}/{model_name}"
    logger.info(f"Loading model from: {gcs_model_path}")

    local_model_path = os.path.basename(gcs_model_path)
    blob = bucket.blob(gcs_model_path)
    blob.download_to_filename(local_model_path)

    # Initialize detector
    logger.info(f"Initializing detector...")
    model = init_detector(config_destintation_path, local_model_path, device="cuda:0")

    bounding_box_annotator = sv.BoundingBoxAnnotator()
    label_annotator = sv.LabelAnnotator()
    img_paths = []
    annotations = {}
    sampled_annotations = {}
    sampled_img_paths = []

    # Get all images paths in bucket
    image_extensions = {".jpg", ".jpeg", ".png"}
    blobs = bucket.list_blobs(prefix=images_path)
    image_files = [
        blob.name
        for blob in blobs
        if any(blob.name.lower().endswith(ext) for ext in image_extensions)
    ]

    # Define Polygon
    polygon = Polygon(cfg.area.polygon)
    logger.info(f"Polygon is {polygon}")

    # Condition to filter older images formats (e.g. that missed the heading in the naming -> legacy), and pictures inside defined polygon
    clean_image_files = []
    for img_blob in image_files:
        parts = os.path.basename(img_blob).split("_")

        # Check the legacy condition
        if len(parts) > 6:
            # Extract latitude and longitude
            lat = float(parts[0])
            lon = float(parts[1])
            point = Point(lat, lon)

            # Check if the point is inside the polygon
            if polygon.contains(point):
                clean_image_files.append(img_blob)

    logger.info(f"Predicting for {len(clean_image_files)} images...")

    batch_images = []
    batch_files = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for i in tqdm.tqdm(
            range(0, len(clean_image_files), batch_size), desc="Processing Batches"
        ):
            logger.info(f"{annotations_filename} - Processing batch index {i}...")

            # Get images
            batch_files = clean_image_files[i : i + batch_size]
            downloaded_data = asyncio.run(fetch_images(bucket, batch_files))

            # Decode them
            batch_images = [
                executor.submit(decode_image, data[0]) for data in downloaded_data
            ]
            batch_images = [future.result() for future in batch_images]

            logger.info(
                f"{cfg.area.name} - {annotations_filename} - Retrieved images for batch index {i}..."
            )

            # Run inference on batch
            result = inference_detector(model, batch_images)

            logger.info(
                f"{cfg.area.name} - {annotations_filename} - Inference run for batch index {i}..."
            )

            try:
                # Pre-process batch
                process_batch(
                    i,
                    batch_images,
                    [data[1] for data in downloaded_data],
                    result,
                    bounding_box_annotator,
                    label_annotator,
                    agnostic_nms,
                    agnostic_nms_thresh,
                    bbox_conf_thresh,
                    keep_only_cpgs,
                    annotations,
                    img_paths,
                    store_predictions,
                    prediction_path,
                    bucket,
                    active_sampling_cfg,
                    sampled_annotations,
                    sampled_img_paths,
                )
            except Exception as e:
                raise e

            batch_images.clear()
            batch_files.clear()

    logger.info(
        f"{cfg.area.name} - {annotations_filename} - Building and exporting detections to GCP..."
    )

    if active_sampling_cfg.enable:
        # Create sampled annotations and upload to GCS
        training_database_path = cfg.training_database_path
        annotation_filename_active_sampling = active_sampling_cfg.annotation_filename
        active_sampling_split = active_sampling_cfg.split
        area_name = cfg.area.name
        sampled_annotations_path = (
            f"{training_database_path}/{area_name}/annotations/{active_sampling_split}"
        )
        create_upload_annotations(
            model,
            sampled_img_paths,
            sampled_annotations,
            bucket,
            sampled_annotations_path,
            annotation_filename_active_sampling,
        )

    # Create annotations and upload to GCS
    create_upload_annotations(
        model, img_paths, annotations, bucket, annotations_path, annotations_filename
    )

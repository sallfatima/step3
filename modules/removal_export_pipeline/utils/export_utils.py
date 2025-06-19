import base64
import os
import string
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from io import BytesIO
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import supervision as sv
import tqdm
from cloud_utils import get_bucket, read_image_from_gcs_opencv, upload_dataframe_to_gcs
from config_model import SetupConfig
from dataset_utils import CloudDetectionDataset
from general_utils import filepath_from_roboflow
from google.api_core.exceptions import ServiceUnavailable
from google.cloud.storage import Bucket
from logger import logger
from numpy.typing import NDArray
from PIL import Image


def find_last(text: str, pattern: str) -> int:
    """Finds last occurrence of pattern in text"""
    return text.rfind(pattern)


def find_second_last(text: str, pattern: str) -> int:
    """Finds second to last occurrence of pattern in text"""
    return text.rfind(pattern, 0, find_last(text, pattern))


def replace_in_positions(
    text: str, target_char: str, replacement_char: str, positions: Tuple
) -> str:
    """Replaces any index occurrence found in positions of 'target_char' in 'text', with 'replacement_char'"""
    count = 0
    new_string = ""
    for char in text:
        if char == target_char:
            count += 1
            if count in list(positions):
                new_string += replacement_char
            else:
                new_string += char
        else:
            new_string += char
    return new_string


def find_indices_of_elements(
    array: NDArray[np.str_], elements: List[str]
) -> List[NDArray[np.int64]]:
    """Finds indices for each element from 'elements' list, in array"""
    array_lower = np.char.lower(array)
    indices = []
    for element in elements:
        indices.append(np.where(array_lower == element.lower())[0])
    return indices


def extract_string_between_substrings(
    input_string: str, start_substring: str, end_substring: str
) -> Optional[str]:
    """Extracts the 'input_string' between 'start_substring' and 'end_substring'"""
    start_index = input_string.find(start_substring)
    if start_index != -1:
        start_index += len(start_substring)
        end_index = input_string.find(end_substring, start_index)
        if end_index != -1:
            return input_string[start_index:end_index]
    return None


def alter_coordinates(group, displacement_degree: float):
    # Get the original latitude and longitude values
    original_lat = group["Latitude"].iloc[0]
    original_lon = group["Longitude"].iloc[0]

    # Randomly alter latitude and longitude for all rows except the first one
    alter_lat = []
    alter_lon = []
    current_displacement = displacement_degree
    for i in range(len(group.index[1:])):
        option = i % 7
        full_round = 1 + i // 7
        if full_round > 1:
            current_displacement = displacement_degree * full_round
        if option == 0:
            alter_lat.append(original_lat + current_displacement)
            alter_lon.append(original_lon)
        elif option == 1:
            alter_lat.append(original_lat - current_displacement)
            alter_lon.append(original_lon)
        elif option == 2:
            alter_lat.append(original_lat)
            alter_lon.append(original_lon + current_displacement)
        elif option == 3:
            alter_lat.append(original_lat)
            alter_lon.append(original_lon - current_displacement)
        elif option == 4:
            alter_lat.append(original_lat + current_displacement)
            alter_lon.append(original_lon + current_displacement)
        elif option == 5:
            alter_lat.append(original_lat - current_displacement)
            alter_lon.append(original_lon - current_displacement)
        elif option == 6:
            alter_lat.append(original_lat - current_displacement)
            alter_lon.append(original_lon + current_displacement)
        elif option == 7:
            alter_lat.append(original_lat + current_displacement)
            alter_lon.append(original_lon - current_displacement)

    # Assign shuffled values back to the DataFrame
    group.loc[group.index[1:], "Latitude"] = alter_lat
    group.loc[group.index[1:], "Longitude"] = alter_lon

    # Return the modified group
    return group


def displace_coordinates(df: pd.DataFrame, displacement: int = 2):
    # Earth's radius in meters
    r = 6378137

    # Convert displacement from meters to degrees (approximately)
    displacement_deg = (displacement / r) * (180 / np.pi)

    # Identifying duplicate rows based on latitude and longitude
    subset_columns = ["Latitude", "Longitude"]
    df = df.groupby(subset_columns, as_index=False).apply(
        alter_coordinates, displacement_deg
    )

    return df


def get_image_year(img_name: str, from_roboflow_platform: bool = False) -> str:
    """Parses year from an image name, depending on where the image comes from"""

    if from_roboflow_platform:
        img_name = img_name.split(".rf")[0]
        index = find_second_last(img_name, "_")
    else:
        index = find_last(img_name, "_")

    year = img_name[index + 1 :].split("-")[0]

    return year


def set_annotated_image_path(img_path: str, from_roboflow: bool = False) -> str:
    """Build the annotated image path, depending on where the image comes from"""

    img_name = os.path.basename(img_path)
    if from_roboflow:
        img_name = f"{os.path.dirname(img_path)}/roboflow_annotated_{img_name}"
    else:
        img_name = f"{os.path.dirname(img_path)}/annotated_{img_name}"

    return img_name


def get_image_info(img_name: str, from_roboflow: bool = False) -> Tuple[str, ...]:
    """Retrieves data related to image, from image name, depending on where the image comes from"""

    if from_roboflow:
        lat, lon = img_name.split("_")[2:4]
        heading, side = img_name.split("_")[4:6]

    else:
        lat, lon = img_name.split("_")[1:3]
        heading, side = img_name.split("_")[3:5]

    date = img_name.split("_")[-1].split(".")[0]

    return lat, lon, heading, side, date


def process_image(
    img_path,
    detections,
    bucket,
    from_roboflow,
    classes,
    capped_letters,
    bounding_box_annotator,
    label_annotator,
    cfg,
    public_image,
    export_cfg,
    ann_name_core,
    roboflow_chr=None,
    roboflow_positions=None,
):
    # Get image array
    if from_roboflow:
        img_path = filepath_from_roboflow(img_path, roboflow_chr, roboflow_positions)

    # Read image from cloud
    image = read_image_from_gcs_opencv(bucket, img_path)

    # Filter detections by class
    good_indices = np.isin(
        detections.class_id,
        find_indices_of_elements(np.array(classes), export_cfg.classes_to_export),
    )
    good_indices = np.where(good_indices)[0]
    classes_detections = detections[good_indices]

    # Skip images with no bounding boxes of classes of interest
    if len(classes_detections) == 0:
        return None

    # Draw bounding box and label on image
    annotated_frame = bounding_box_annotator.annotate(
        scene=image.copy(), detections=classes_detections
    )
    annotated_frame = label_annotator.annotate(
        scene=annotated_frame.copy(),
        detections=classes_detections,
        labels=[
            f"{capped_letters[i]}: {classes[det[3]]}"
            for det, i in zip(classes_detections, good_indices)
        ],
    )

    # Convert annotated frame to RGB
    annotated_frame = annotated_frame[..., [2, 1, 0]]
    annotated_frame = Image.fromarray(annotated_frame, mode="RGB")

    # Set name for saving
    img_path = set_annotated_image_path(img_path, from_roboflow)

    # Get blob depending on the bucket status (private or public)
    if public_image:
        destination_blob_name = (
            f"{os.path.dirname(img_path)}/{ann_name_core}/{os.path.basename(img_path)}"
        )
        blob = get_bucket(cfg.public_bucket_name, cfg.project_id).blob(
            destination_blob_name
        )
        # Check for existence of this blob, and raise error to not overwrite it, if so
        if blob.exists():
            text_error = (
                f"The image {destination_blob_name} already exists."
                f" Change the annotation filename which will be used as the directory to store the images"
            )
            logger.error(text_error)
            raise ValueError(text_error)
    else:
        destination_blob_name = f"{os.path.dirname(img_path)}/annotated_images/{ann_name_core}/{os.path.basename(img_path)}"
        blob = bucket.blob(destination_blob_name)
        # Check for existence of this blob, and raise error to not overwrite it, if so
        if blob.exists():
            text_error = (
                f"The image {destination_blob_name} already exists."
                f" Change the annotation filename which will be used as the directory to store the images"
            )
            logger.error(text_error)
            raise ValueError(text_error)

    # Upload annotated blob
    buffer = BytesIO()
    annotated_frame.save(buffer, format="JPEG")
    buffer.seek(0)

    n_attempts = 0
    while n_attempts < 5:
        try:
            buffer.seek(0)  # Reset stream before each upload attempt
            blob.upload_from_file(buffer, content_type="image/jpeg")
            break  # Exit loop if upload is successful
        except ServiceUnavailable as e:
            logger.warning(
                f"Upload failed for {destination_blob_name}, attempt {n_attempts + 1}"
            )
            time.sleep(5)
            n_attempts += 1

    if public_image:
        url = f"https://storage.googleapis.com/{cfg.public_bucket_name}/{destination_blob_name}"
    else:
        # Generate a signed URL valid for 7 days (10079 minutes)
        url = blob.generate_signed_url(
            version="v4",
            expiration=datetime.utcnow() + timedelta(minutes=10079),
            method="GET",
        )

    # Extract info from image name
    img_name = os.path.basename(img_path)
    lat, lon, heading, side, date = get_image_info(img_name, from_roboflow)

    excel_rows = []
    map_data_rows = []

    for det, i in zip(classes_detections, good_indices):
        # Get class
        det_class = classes[det[3]]
        letter = capped_letters[i]

        # Prepare Excel data
        excel_rows.append(
            [
                img_path,
                letter,
                det_class,
                date,
                float(lat),
                float(lon),
                list(det[0]),
                url,
            ]
        )

        # Prepare My Maps data
        sample_string_id = f"{lat}_{lon}_{letter}_{os.path.basename(img_path)}"
        encoded_bytes = base64.urlsafe_b64encode(sample_string_id.encode("utf-8"))
        sample_id = encoded_bytes.decode("utf-8")

        map_data_rows.append(
            [sample_id, float(lat), float(lon), letter, det_class, date, url]
        )

    return excel_rows, map_data_rows


def export_to_client(cfg: SetupConfig, bucket: Bucket) -> None:
    """
    Exports the detections to Google My Maps and Excel file, as client deliverables
    Args:
        cfg: configuration dictionary
        bucket: GCS Bucket object
    """

    # Get images path
    images_path = cfg.area.images_path

    # Get config file for export action
    export_cfg = cfg.removal_export.export

    destination_path = export_cfg.output_path
    shop_displacement = export_cfg.shop_displacement
    from_roboflow = export_cfg.from_roboflow
    roboflow_chr = export_cfg.from_roboflow_character
    roboflow_positions = tuple(export_cfg.from_roboflow_positions)
    public_image = export_cfg.public
    ann_name_base = cfg.annotations_filename
    annotation_filename_to_export = export_cfg.annotation_filename_to_export

    # Define annotations
    if annotation_filename_to_export is None:
        ann_name_core = ann_name_base.split(".")[0]
        annotation_json_path = f"{cfg.area.annotations_path}/{ann_name_core}_no_duplicates_image_location.json"

    else:
        ann_name_core = annotation_filename_to_export.split(".")[0]
        annotation_json_path = f"{cfg.area.annotations_path}/{ann_name_core}.json"

    excel_out_path = f"{cfg.database_path}/{cfg.area.name}/{destination_path}/results_{ann_name_core}.xlsx"
    csv_out_path = f"{cfg.database_path}/{cfg.area.name}/{destination_path}/results_{ann_name_core}.csv"

    # Get capped letters
    capped_letters = [letter for letter in string.ascii_uppercase]

    # Define dataset and bounding box annotator
    dataset = CloudDetectionDataset.from_coco(
        images_directory_path=images_path,
        annotations_path=annotation_json_path,
        bucket=bucket,
    )

    # Get classes
    classes = dataset.classes

    # Compute stats
    nr_per_class = {key: 0 for key in range(len(classes))}
    year_dict = {}

    for img_name, detections in tqdm.tqdm(dataset.annotations.items()):

        # Get image name
        img_name_cloud = os.path.basename(img_name)

        # Get year
        year = get_image_year(img_name_cloud, from_roboflow)

        # Increment yearly images
        if year_dict.get(year, None):
            year_dict[year] += 1
        else:
            year_dict[year] = 1

        for class_id in nr_per_class.keys():

            # Filter detections by class and count them
            class_detections = detections[detections.class_id == class_id]
            nr_per_class[class_id] += len(class_detections)

    # Convert keys to classes
    nr_per_class = {classes[k]: v for k, v in nr_per_class.items()}

    logger.info(f"Detections per class:\n {nr_per_class}")
    logger.info(f"Images by year:\n {year_dict}")

    bounding_box_annotator = sv.BoundingBoxAnnotator()
    label_annotator = sv.LabelAnnotator(
        text_position=sv.Position.TOP_LEFT, text_scale=0.25, text_padding=5
    )

    # Define columns for excel
    columns_excel = [
        "file name",
        "bounding box ID",
        "class",
        "date",
        "latitude",
        "longitude",
        "bounding box",
        "temporary cloud link",
    ]
    columns_map_data = [
        "ID",
        "Latitude",
        "Longitude",
        "Bounding Box ID",
        "Class",
        "Date",
        "PhotoURL",
    ]

    logger.info(f"Started annotating images for export...")

    # Use ThreadPoolExecutor to process images in parallel
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for img_path, detections in dataset.annotations.items():
            futures.append(
                executor.submit(
                    process_image,
                    img_path,
                    detections,
                    bucket,
                    from_roboflow,
                    classes,
                    capped_letters,
                    bounding_box_annotator,
                    label_annotator,
                    cfg,
                    public_image,
                    export_cfg,
                    ann_name_core,
                    roboflow_chr,
                    roboflow_positions,
                )
            )

        results = [future.result() for future in futures if future.result() is not None]

    logger.info(f"Gathering results...")

    # Collect results
    all_excel_rows = []
    all_map_data_rows = []
    for excel_rows, map_data_rows in results:
        all_excel_rows.extend(excel_rows)
        all_map_data_rows.extend(map_data_rows)

    # Convert to DataFrames
    excel_df = pd.DataFrame(all_excel_rows, columns=columns_excel)
    map_data_df = pd.DataFrame(all_map_data_rows, columns=columns_map_data)

    # Save Excel to GCS
    logger.info(f"Saving to GCS...")

    upload_dataframe_to_gcs(bucket, excel_df, excel_out_path, export_format="excel")

    # Displace duplicates coordinates of shops images by 'shop_displacement' meters and save to CSV
    map_data_df = displace_coordinates(map_data_df, displacement=shop_displacement)

    # Split the DataFrame into batches
    # Note: maximum number of data points in My Maps is 2000
    batch_size = 2000
    batches = [
        map_data_df[i : i + batch_size] for i in range(0, len(map_data_df), batch_size)
    ]

    # Get path
    annotated_csv_out = os.path.basename(csv_out_path)
    annotated_csv_directory_path = os.path.dirname(csv_out_path)

    for i, batch in enumerate(batches):
        out_name = annotated_csv_out.split(".")[0]
        save_name = f"{out_name}_{i}.csv"
        save_path = f"{annotated_csv_directory_path}/{save_name}"
        upload_dataframe_to_gcs(bucket, batch, save_path, export_format="csv")

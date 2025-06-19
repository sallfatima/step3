import ast
import json
import os
import random
from collections import Counter
from typing import Optional, Tuple, Union

import numpy as np
import pandas as pd
import supervision as sv
from cloud_utils import upload_json_to_gcs
from config_model import SetupConfig
from dataset_utils import CloudDetectionDataset, DetectionDataset
from google.cloud.storage import Bucket
from location_estimator.aggregate import aggregate
from location_estimator.latlng import LatLng
from location_estimator.mapping import Annotation, City, Image
from logger import logger


def run_removal_location(
    cfg: SetupConfig,
    bucket: Bucket,
    bbox: Tuple[float, ...],
    img_ann: str = "image_data.csv",
    det_ann: str = "annotations_data.csv",
    det_ann_estimations: str = "annotations_data_estimation.csv",
    det_ann_estimations_aggregated: str = "annotations_data_estimation_aggregated.csv",
) -> None:
    """
    Finds and deleted duplicate instances of shops
    Args:
        cfg: config dictionary
        bucket: instance of GCP bucket
        bbox: The bounding box of the area, in (min_lat, min_lon, max_lat, max_lon) format
        img_ann: path to intermediary annotation file, at image level
        det_ann: path to intermediary annotation file, at detections level
        det_ann_estimations: path to intermediary annotation file with estimated locations
        det_ann_estimations_aggregated: path of aggregated annotations file
    """

    # Path to images (and annotation file) in cloud
    images_path = cfg.area.images_path
    annotations_path = cfg.area.annotations_path

    # Get duplicate removal configuration
    duplicate_cfg = cfg.removal_export.location_removal
    ann_base_name = cfg.annotations_filename

    # Get config parameters
    local_ann_file_path = duplicate_cfg.local_ann_file_path
    class_name = duplicate_cfg.class_name

    # Define dataset
    if local_ann_file_path:
        if "no_duplicates_image" not in local_ann_file_path:
            raise ValueError(
                "Annotation file should be the result of feature based removal,"
                " and contain a no_duplicates_image suffix."
            )
        dataset = sv.DetectionDataset.from_coco(
            images_directory_path=os.path.dirname(local_ann_file_path),
            annotations_path=local_ann_file_path,
        )
    else:
        dataset = CloudDetectionDataset.from_coco(
            images_directory_path=images_path,
            annotations_path=f"{annotations_path}/{ann_base_name.split('.')[0]}_no_duplicates_image.json",
            bucket=bucket,
        )

    logger.info("Dataset loaded")

    # Get buildings path
    country_path = "/".join(cfg.area.name.split("/")[:2])
    building_filename = f"gs://{cfg.bucket_name}/{cfg.buildings_database_path}/{country_path}/open_buildings_v3_polygons_ne_110m.csv.gz"

    # Find duplicate instances
    find_duplicates(
        dataset,
        bbox,
        class_name,
        img_ann,
        det_ann,
        det_ann_estimations,
        det_ann_estimations_aggregated,
        building_filename,
    )

    # Remove duplicates from the annotation file
    remove_duplicates(
        dataset,
        det_ann_estimations_aggregated,
        local_ann_file_path,
        annotations_path,
        ann_base_name,
        bucket,
    )

    # Remove intermediary files
    files_to_remove = [
        img_ann,
        det_ann,
        det_ann_estimations,
        det_ann_estimations_aggregated,
    ]
    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)


def find_duplicates(
    dataset: Union[CloudDetectionDataset, DetectionDataset],
    bbox: Tuple[float, ...],
    class_name: str,
    img_ann: str,
    det_ann: str,
    det_ann_estimations: str,
    det_ann_estimations_aggregated: str,
    building_file_path: str,
) -> None:

    # Create the annotation files in the needed format
    logger.info("Creating annotations for image and detections...")
    create_annotation_csvs(dataset, img_ann, det_ann, class_name)

    # Estimate locations
    logger.info("Estimating locations of detections...")
    estimate_locations(img_ann, det_ann, det_ann_estimations, bbox, building_file_path)

    # Aggregate shops locations
    logger.info("Aggregating detections...")
    aggregate_shops(det_ann_estimations, det_ann_estimations_aggregated)


def get_indexes(classes, query) -> np.ndarray:
    index_map = {element.lower(): i for i, element in enumerate(classes)}
    return np.array(
        [index_map[item.lower()] for item in query if item.lower() in index_map]
    )


def create_annotation_csvs(
    dataset: Union[CloudDetectionDataset, DetectionDataset],
    img_ann: str,
    det_ann: str,
    class_name: str,
) -> None:
    # Initialize data
    data = []
    annotations_data = []

    # Iterate over the images in the dataset
    for i, (image_path, _, annotations) in enumerate(dataset):

        # Parse the image name to get lat, lon, heading, and fov
        image_name = os.path.basename(image_path)
        lat, lon, heading_index, side_index, heading, fov, date = image_name[:-4].split(
            "_"
        )
        lat = float(lat)
        lon = float(lon)
        heading = float(heading)
        fov = float(fov)

        # Count annotations for class ID 0
        specific_class_detections = annotations[
            np.isin(annotations.class_id, get_indexes(dataset.classes, [class_name]))
        ]
        number_annotations_per_image = len(specific_class_detections)

        if number_annotations_per_image > 0:
            # Create a dictionary for the current image's data
            data.append(
                {
                    "image_id": image_path,
                    "x": lon,
                    "y": lat,
                    "pitch": 0,
                    "heading": heading,
                    "height": 1.979,
                    "fov": fov,
                    "annotations": number_annotations_per_image,
                }
            )

            # Extract annotations for the current image
            for annotation_index, annotation in enumerate(annotations):
                if int(annotation[3]) == get_indexes(dataset.classes, [class_name])[0]:
                    top, left, bottom, right = annotation[0]
                    annotations_data.append(
                        {
                            "id": annotation_index,
                            "image_id": image_path,
                            "label_id": int(annotation[3]),
                            "top": float(top),
                            "left": float(left),
                            "bottom": float(bottom),
                            "right": float(right),
                        }
                    )

    # Create a DataFrame from the collected data
    df = pd.DataFrame(data)
    df_annotations = pd.DataFrame(annotations_data)

    # Save to CSV
    df.to_csv(img_ann, index=False)
    df_annotations.to_csv(det_ann, index=False)


def estimate_locations(
    img_ann: str,
    det_ann: str,
    annotation_data_estimations_path: str,
    bbox: Tuple[float, ...],
    building_file_path: str,
) -> None:
    # Create the city object
    city = City(building_file_path, bbox)

    # Read image level data
    df = pd.read_csv(img_ann)

    def create_image(r):
        return Image(
            r.index,
            float(r["x"]),
            float(r["y"]),
            float(r["pitch"]),
            float(r["heading"]),
            float(r["height"]),
            float(r["fov"]),
            int(r["annotations"]),
            width=640,
            height=640,
        )

    images = df.set_index("image_id").apply(create_image, axis=1).to_dict()

    # Read the annotations CSV into a DataFrame
    df_annotations = pd.read_csv(det_ann)

    def row_estimate(r):
        a = Annotation(
            r["image_id"],
            r["label_id"],
            int(float(r["left"])),
            int(float(r["top"])),
            int(float(r["right"])),
            int(float(r["bottom"])),
        )
        # Check if the image exists in 'images' and country is 'nigeria'
        if r["image_id"] in images:
            # Append the annotation to the image
            images[r["image_id"]].append_annotation(a)

            # Get the estimated location
            location = city.locate(a)
            if location:
                return pd.Series([location[1], location[0]])
        return pd.Series([None, None])

    # Apply the function to each row and get 'est_lat' and 'est_lng'
    df_annotations[["est_lat", "est_lng"]] = df_annotations.apply(row_estimate, axis=1)

    # Write the final DataFrame to CSV, including the new columns
    df_annotations.to_csv(annotation_data_estimations_path, index=False)


def aggregate_shops(
    det_ann_estimations: str, det_ann_estimations_aggregated: str
) -> None:

    # Read the CSV into a pandas DataFrame
    df_annotations = pd.read_csv(det_ann_estimations)

    # Filter rows where lat and lng columns are not empty
    df_filtered = df_annotations.dropna(subset=["est_lat", "est_lng"])

    # Create a list to store LatLng objects
    points = []

    def create_latlng(r):
        ll = LatLng(float(r["est_lat"]), float(r["est_lng"]))
        ll.label = int(r["label_id"])  # Adjust 'some_label_column' to match your data
        ll.image = r["image_id"]
        ll.id = int(r["id"])  # Adjust 'annotation_id' to match your data
        points.append(ll)

    # Apply the function to each filtered row
    df_filtered.apply(create_latlng, axis=1)

    # Call aggregate on the points list
    agg_points = aggregate(points)

    # Log the result
    logger.info(f"After aggregation, {len(agg_points)} store locations were left.")

    # Prepare data for writing to CSV
    agg_data = [
        {
            "lat": p.lat,
            "lng": p.lng,
            "labels": p.label,
            "image_ids": p.image,
            "annotation_ids": p.id,
        }
        for p in agg_points
    ]

    # Create a new DataFrame from aggregated data
    df_agg = pd.DataFrame(agg_data)

    # Write the aggregated data to a new CSV
    df_agg.to_csv(det_ann_estimations_aggregated, index=False)


def process_row(row) -> Optional[pd.DataFrame]:
    image_ids = row["image_ids"]
    annotation_ids = row["annotation_ids"]

    # Case 1: If there is a single image_id, remove the row
    if len(image_ids) == 1:
        return None

    # Case 2: If all image_ids are the same, remove the row
    if len(set(image_ids)) == 1:
        return None

    # Case 3: If there are 2 image_ids
    if len(image_ids) == 2:
        # If the image_ids are not the same, keep one random image_id and its annotation
        selected_index = random.choice([0, 1])
        return pd.DataFrame(
            {
                "image_ids": [[image_ids[selected_index]]],
                "annotation_ids": [[annotation_ids[selected_index]]],
            }
        )

    # Case 4: If there are 3 image_ids
    if len(image_ids) == 3:
        # If all are different, keep 2 random image_ids and annotation_ids
        if len(set(image_ids)) == 3:
            selected_indices = random.sample([0, 1, 2], 2)
            return pd.DataFrame(
                {
                    "image_ids": [[image_ids[i] for i in selected_indices]],
                    "annotation_ids": [[annotation_ids[i] for i in selected_indices]],
                }
            )
        # If there is a majority (2/3), keep the minority
        majority_id = max(set(image_ids), key=image_ids.count)
        if image_ids.count(majority_id) == 2:
            minority_index = [i for i, x in enumerate(image_ids) if x != majority_id][0]
            return pd.DataFrame(
                {
                    "image_ids": [[image_ids[minority_index]]],
                    "annotation_ids": [[annotation_ids[minority_index]]],
                }
            )

    # Case 5: If there are 4 image_ids
    if len(image_ids) == 4:
        # If all are different, keep 3 random image_ids and annotation_ids
        if len(set(image_ids)) == 4:
            selected_indices = random.sample([0, 1, 2, 3], 3)
            return pd.DataFrame(
                {
                    "image_ids": [[image_ids[i] for i in selected_indices]],
                    "annotation_ids": [[annotation_ids[i] for i in selected_indices]],
                }
            )
        # If 3 out of 4 are the same, keep the minority
        majority_id = max(set(image_ids), key=image_ids.count)
        if image_ids.count(majority_id) == 3:
            minority_index = [i for i, x in enumerate(image_ids) if x != majority_id][0]
            return pd.DataFrame(
                {
                    "image_ids": [[image_ids[minority_index]]],
                    "annotation_ids": [[annotation_ids[minority_index]]],
                }
            )
        # If 2 pairs of image_ids, keep the 2 different ones
        elif image_ids.count(majority_id) == 2:
            unique_ids = list(set(image_ids))
            if len(unique_ids) == 3:
                # 2 are the same and 2 are different
                different_indices = [
                    i for i, x in enumerate(image_ids) if x != majority_id
                ]
                return pd.DataFrame(
                    {
                        "image_ids": [[image_ids[i] for i in different_indices]],
                        "annotation_ids": [
                            [annotation_ids[i] for i in different_indices]
                        ],
                    }
                )
            else:
                # If we can't form clear pairs, randomly select one as fallback
                selected_index = random.choice([0, 1, 2, 3])
                return pd.DataFrame(
                    {
                        "image_ids": [[image_ids[selected_index]]],
                        "annotation_ids": [[annotation_ids[selected_index]]],
                    }
                )
    # Case 6: If there are more than 4 image_ids
    if len(image_ids) > 4:
        # Count occurrences of each image_id
        id_counts = Counter(image_ids)
        # Find the most common image_id(s)
        most_common = id_counts.most_common()

        # If all are unique, keep len(image_ids) - 1 random image_ids and annotation_ids
        if len(set(image_ids)) == len(image_ids):
            selected_indices = random.sample(
                list(range(len(image_ids))), len(image_ids) - 1
            )
            return pd.DataFrame(
                {
                    "image_ids": [[image_ids[i] for i in selected_indices]],
                    "annotation_ids": [[annotation_ids[i] for i in selected_indices]],
                }
            )

        # If there is a single majority that appears more than half of the time, keep one of each minority
        elif most_common[0][1] > len(image_ids) / 2:
            minority_indices = [
                i for i, x in enumerate(image_ids) if x != most_common[0][0]
            ]
            selected_indices = random.sample(
                minority_indices, min(2, len(minority_indices))
            )
            return pd.DataFrame(
                {
                    "image_ids": [[image_ids[i] for i in selected_indices]],
                    "annotation_ids": [[annotation_ids[i] for i in selected_indices]],
                }
            )

        # If there are multiple pairs, keep one of each pair
        elif all(count == 2 for _, count in most_common):
            unique_pairs = list(set(image_ids))
            return pd.DataFrame(
                {
                    "image_ids": [[unique_pairs[0], unique_pairs[1]]],
                    "annotation_ids": [
                        [
                            annotation_ids[image_ids.index(unique_pairs[0])],
                            annotation_ids[image_ids.index(unique_pairs[1])],
                        ]
                    ],
                }
            )

        # As a fallback, keep a random subset
        selected_index = random.choice(range(len(image_ids)))
        return pd.DataFrame(
            {
                "image_ids": [[image_ids[selected_index]]],
                "annotation_ids": [[annotation_ids[selected_index]]],
            }
        )


def remove_duplicates(
    dataset: Union[CloudDetectionDataset, DetectionDataset],
    det_ann_estimations_aggregated: str,
    local_ann_file_path: str,
    annotations_path: str,
    ann_base_name: str,
    bucket: Bucket,
) -> None:

    # read aggregated data
    df = pd.read_csv(det_ann_estimations_aggregated)

    df["image_ids"] = df["image_ids"].apply(ast.literal_eval)
    df["annotation_ids"] = df["annotation_ids"].apply(ast.literal_eval)

    # Apply the logic to each row
    processed_rows = df.apply(process_row, axis=1)

    # Concatenate all DataFrames from the processed rows into a single DataFrame
    df_cleaned = pd.concat(processed_rows.dropna().values, ignore_index=True)

    # Initialize an empty dictionary to store the image_id: annotation_id pairs
    for_deletion_dict = {}

    # Loop over each row in the cleaned DataFrame
    for idx, row in df_cleaned.iterrows():
        image_ids = row["image_ids"]
        annotation_ids = row["annotation_ids"]

        # For each image_id and its corresponding annotation_id, add to the dictionary
        for img_id, ann_id in zip(image_ids, annotation_ids):
            if for_deletion_dict.get(img_id, None):
                for_deletion_dict[img_id].append(ann_id)
            else:
                for_deletion_dict[img_id] = [ann_id]

    for image_name, det_ids in for_deletion_dict.items():

        dataset.annotations[image_name].xyxy = np.delete(
            dataset.annotations[image_name].xyxy, det_ids, axis=0
        )
        dataset.annotations[image_name].class_id = np.delete(
            dataset.annotations[image_name].class_id, det_ids, axis=0
        )

        # Remove image completely if no detections left
        if len(dataset.annotations[image_name]) == 0:
            del dataset.annotations[image_name]
            dataset.image_paths.remove(image_name)

    # Export dataset annotations to coco format
    output_path = (
        os.path.dirname(local_ann_file_path)
        if local_ann_file_path
        else annotations_path
    )

    no_duplicates_annotations_filename = (
        f"{ann_base_name.split('.')[0]}_no_duplicates_image_location.json"
    )

    if local_ann_file_path:
        output_filepath = f"{output_path}/{no_duplicates_annotations_filename}"
    else:
        output_filepath = no_duplicates_annotations_filename

    dataset.as_coco(annotations_path=output_filepath)

    # Read annotation json
    with open(output_filepath, "r") as json_file:
        data_dict = json.load(json_file)

    # Upload to GCS
    upload_json_to_gcs(
        bucket, f"{annotations_path}/{no_duplicates_annotations_filename}", data_dict
    )

    # Remove unwanted files
    if os.path.exists("temp_annotations.json"):
        os.remove("temp_annotations.json")

    if not local_ann_file_path:
        os.remove(output_filepath)

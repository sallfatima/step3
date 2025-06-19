import json
import os
import random
from itertools import product
from typing import List, Optional, Tuple, Union

import cv2
import networkx as nx
import numpy as np
import pandas as pd
import supervision as sv
import tqdm
from cloud_utils import (
    read_image_from_gcs_opencv,
    upload_array_as_jpg,
    upload_json_to_gcs,
)
from config_model import SetupConfig
from dataset_utils import CloudDetectionDataset, DetectionDataset
from general_utils import filepath_from_roboflow, replace_in_positions
from google.cloud.storage import Bucket
from image_matching.ui.api import ImageMatchingAPI
from image_matching.ui.utils import DEVICE
from image_matching_conf import DARKFEAT_CONF, DEDODE_CONF, LIGHTGLUE_CONF
from logger import logger
from sklearn.neighbors import BallTree

# Set seed for removing same duplicates each time
random.seed(10)


def run_removal(cfg: SetupConfig, bucket: Bucket):
    """
    Finds and deleted duplicate instances of shops
    Args:
        cfg: config dictionary
        bucket: instance of GCP bucket
    """

    # Define the algorithms for duplicate removal
    configs = [DEDODE_CONF, LIGHTGLUE_CONF, DARKFEAT_CONF]

    apis = []
    for config in configs:
        apis.append(ImageMatchingAPI(conf=config, device=DEVICE))

    # Find duplicate instances
    find_duplicates(cfg, bucket, apis)

    # Remove the instances from the dataset
    remove_duplicates(cfg, bucket)


def get_indexes(classes, query) -> np.ndarray:
    index_map = {element.lower(): i for i, element in enumerate(classes)}
    return np.array(
        [index_map[item.lower()] for item in query if item.lower() in index_map]
    )


def find_neighbors(
    dataset: Union[CloudDetectionDataset, DetectionDataset],
    export_classes_name: List[str],
    distance_meters: int = 30,
    from_roboflow: bool = False,
    roboflow_chr: Optional[str] = None,
    roboflow_positions: Optional[Tuple[int]] = None,
) -> pd.DataFrame:

    # Define columns
    columns_data = [
        "img_name",
        "latitude",
        "longitude",
        "latitude_rad",
        "longitude_rad",
    ]
    data_df = pd.DataFrame([], columns=columns_data)

    for img_path, _, detections in tqdm.tqdm(dataset):

        # Rebuild image name (Roboflow alters it at export time)
        if from_roboflow:
            img_path = filepath_from_roboflow(
                img_path, roboflow_chr, roboflow_positions
            )

        img_name_cloud = os.path.basename(img_path)

        # Get lat/lon from filename
        lat, lon = img_name_cloud.split("_")[:2]

        # Filter detections by class
        detections = detections[
            np.isin(
                detections.class_id, get_indexes(dataset.classes, export_classes_name)
            )
        ]

        # Skip images with no bounding boxes of classes of interest
        if len(detections) == 0:
            continue

        # Update dataframe
        new_row = pd.DataFrame(
            [
                [
                    img_path,
                    float(lat),
                    float(lon),
                    np.deg2rad(float(lat)),
                    np.deg2rad(float(lon)),
                ]
            ],
            columns=columns_data,
        )
        data_df = pd.concat([data_df, new_row], ignore_index=True)

    # Construct the ball tree.
    ball = BallTree(
        data_df[["latitude_rad", "longitude_rad"]].values, metric="haversine"
    )

    # km / earth_radius_in_km
    radius = (distance_meters / 1000) / 6371

    data_df["neighbors"] = None
    info_df = data_df[["latitude_rad", "longitude_rad"]]

    for i, row in info_df.iterrows():

        # Query by distance
        indices = ball.query_radius(row.values.reshape(1, -1), r=radius)[0]

        # It means that only the point itself is returned, if length equals 1
        if len(indices) > 1:
            # if not isinstance(indices, list):
            if isinstance(indices, np.ndarray):
                indices = indices.tolist()
            else:
                indices = list(indices)

        # Set neighbours lists
        data_df.at[i, "neighbors"] = indices

    # Drop unwanted columns
    data_df = data_df.drop(columns=["latitude_rad", "longitude_rad"])

    return data_df


def reshape_crop(crop, minimum_side_dimension: int = 100):
    """Reshapes the crop to a bigger dimension if any of the image sides is less than  minimum_side_dimension"""

    height, width = crop.shape[:2]
    if width < minimum_side_dimension or height < minimum_side_dimension:
        # Calculate the scaling factor
        scale_factor = max(
            minimum_side_dimension / width, minimum_side_dimension / height
        )

        # Compute the new dimensions
        new_width = int(width * scale_factor)
        new_height = int(height * scale_factor)

        crop = cv2.resize(
            crop[..., [0, 1, 2]],
            (new_width, new_height),
            interpolation=cv2.INTER_LINEAR,
        )[..., [0, 1, 2]]

    return crop


def find_duplicates(cfg, bucket, apis):

    # Path to images (and annotation file) in cloud
    images_path = cfg.area.images_path
    annotations_path = cfg.area.annotations_path

    duplicate_cfg = cfg.removal_export.image_removal

    # Get config parameters
    export_classes_name = duplicate_cfg.export_classes_name
    neighbor_distance_meters = duplicate_cfg.neighbor_distance_meters
    viz_duplicate_crops = duplicate_cfg.save_duplicate_crops
    duplicate_crops_dir = duplicate_cfg.duplicate_crops_dir
    from_roboflow = duplicate_cfg.from_roboflow
    roboflow_chr = duplicate_cfg.from_roboflow_character
    roboflow_positions = tuple(duplicate_cfg.from_roboflow_positions)
    local_ann_file_path = duplicate_cfg.local_ann_file_path
    ann_name_base = cfg.annotations_filename

    # Define dataset
    if local_ann_file_path:
        dataset = sv.DetectionDataset.from_coco(
            images_directory_path=os.path.dirname(local_ann_file_path),
            annotations_path=local_ann_file_path,
        )
    else:
        dataset = CloudDetectionDataset.from_coco(
            images_directory_path=images_path,
            annotations_path=f"{annotations_path}/{ann_name_base}",
            bucket=bucket,
        )

    logger.info("Dataset loaded")

    # Find neighbouring images
    neighbors_df = find_neighbors(
        dataset,
        export_classes_name,
        distance_meters=neighbor_distance_meters,
        from_roboflow=from_roboflow,
        roboflow_chr=roboflow_chr,
        roboflow_positions=roboflow_positions,
    )
    logger.info("Neighbours found")

    # Explode neighbors lists
    exploded_neighbors_df = neighbors_df.explode("neighbors")
    exploded_neighbors_df["neighbors"] = exploded_neighbors_df["neighbors"].astype(int)
    exploded_neighbors_df = exploded_neighbors_df.merge(
        neighbors_df["img_name"], left_on="neighbors", right_index=True
    )
    # Create pairs of close images
    string_pairs = exploded_neighbors_df[["img_name_x", "img_name_y"]].values.tolist()

    # Create a set of image pairs using a set comprehension
    image_pairs = {tuple(sorted(pair)) for pair in string_pairs}

    # Set duplicates dataframe
    columns_duplicates = ["Image1", "Detection ID1", "Image2", "Detection ID2"]
    duplicates_df = pd.DataFrame([], columns=columns_duplicates)

    logger.info("Finding duplicates...")

    pair_index = 0
    for img_path1, img_path2 in tqdm.tqdm(image_pairs):

        # Get all annotations
        anns1 = dataset.annotations[img_path1]
        anns2 = dataset.annotations[img_path2]

        if local_ann_file_path:
            img1 = cv2.imread(img_path1)
            img2 = cv2.imread(img_path2)
        else:
            img_name1_cloud = img_path1
            img_name2_cloud = img_path2

            # Change name if annotations come from Roboflow
            if from_roboflow:
                img_name1_cloud = filepath_from_roboflow(
                    img_path1, roboflow_chr, roboflow_positions
                )
                img_name2_cloud = filepath_from_roboflow(
                    img_path2, roboflow_chr, roboflow_positions
                )

            # Read images from cloud
            img1 = read_image_from_gcs_opencv(bucket, img_name1_cloud)
            img2 = read_image_from_gcs_opencv(bucket, img_name2_cloud)

        for class_name in export_classes_name:
            # Filter detections by class
            class_indices1 = np.isin(
                anns1.class_id, get_indexes(dataset.classes, [class_name])
            )
            class_indices2 = np.isin(
                anns2.class_id, get_indexes(dataset.classes, [class_name])
            )

            # Get all possible combinations from a class
            combinations = list(
                product(np.where(class_indices1)[0], np.where(class_indices2)[0])
            )

            for combination in combinations:

                # Get annotations
                ann1 = anns1[int(combination[0])]
                ann2 = anns2[int(combination[1])]

                # Get bounding boxes
                box1 = ann1.xyxy[0].astype(int)
                box2 = ann2.xyxy[0].astype(int)

                # Get cropped images according to bounding boxes
                crop1 = img1[box1[1] : box1[3], box1[0] : box1[2]]
                crop2 = img2[box2[1] : box2[3], box2[0] : box2[2]]

                # Bypass mistakes in annotation
                if (
                    crop1.shape[0] == 0
                    or crop1.shape[1] == 0
                    or crop2.shape[0] == 0
                    or crop2.shape[1] == 0
                ):
                    continue

                # Crops as RGBs
                crop1 = crop1[..., [2, 1, 0]]
                crop2 = crop2[..., [2, 1, 0]]

                # Run the duplicate check
                votes = 0
                for api in apis:

                    # NOTE: For DeDoDe, this was needed to bypass errors related to image size
                    if api.conf["feature"]["model"]["name"] in ["dedode"]:
                        crop1 = reshape_crop(crop1, minimum_side_dimension=100)
                        crop2 = reshape_crop(crop2, minimum_side_dimension=100)

                    # TODO: Check here for OpenCV error in logs. Check which API throws it, and at which step.
                    try:
                        api(crop1, crop2)
                        nr_matches_ransac = api.get_nr_matches()
                    except Exception as e:
                        logger.warning(f"Exception: {e}")
                        nr_matches_ransac = 0

                    if api.conf["threshold"] < nr_matches_ransac:
                        votes += 1

                if votes >= (len(apis) // 2) + 1:
                    # Consider it as good, and add it to the dataframe
                    new_row = pd.DataFrame(
                        [
                            [
                                img_path1,
                                int(combination[0]),
                                img_path2,
                                int(combination[1]),
                            ]
                        ],
                        columns=columns_duplicates,
                    )
                    duplicates_df = pd.concat(
                        [duplicates_df, new_row], ignore_index=True
                    )

                    # Visualize crops
                    if viz_duplicate_crops:

                        crop1_path = f"{cfg.area.viz_path}/{duplicate_crops_dir}/pair_{pair_index}_id_0_votes_{votes}.jpg"
                        crop2_path = f"{cfg.area.viz_path}/{duplicate_crops_dir}/pair_{pair_index}_id_1_votes_{votes}.jpg"
                        upload_array_as_jpg(bucket, crop1_path, crop1)
                        upload_array_as_jpg(bucket, crop2_path, crop2)

                    # Increase pair_index
                    pair_index += 1

    # Write to file
    duplicates_df.to_csv(f"temp_duplicates.csv", index=False)


def remove_duplicates(cfg, bucket):
    logger.info("Removing duplicates...")

    # Path to images (and annotation file) in cloud
    images_path = cfg.area.images_path
    annotations_path = cfg.area.annotations_path

    # Get duplicate removal config
    duplicate_cfg = cfg.removal_export.image_removal
    local_ann_file_path = duplicate_cfg.local_ann_file_path
    ann_name_base = cfg.annotations_filename

    # Get dataframe of duplicates
    duplicates_df = pd.read_csv("temp_duplicates.csv")

    # Define annotations file
    if local_ann_file_path:
        # Define dataset and bounding box annotator
        dataset = sv.DetectionDataset.from_coco(
            images_directory_path=os.path.dirname(local_ann_file_path),
            annotations_path=local_ann_file_path,
        )
    else:
        dataset = CloudDetectionDataset.from_coco(
            images_directory_path=images_path,
            annotations_path=f"{annotations_path}/{ann_name_base}",
            bucket=bucket,
        )
    logger.info("Dataset loaded")

    # Initiate graph
    g = nx.Graph()

    # Add edges based on DataFrame rows
    for _, row in duplicates_df.iterrows():
        g.add_edge(
            (row["Image1"], row["Detection ID1"]), (row["Image2"], row["Detection ID2"])
        )

    # Find connected components (groups of similar objects)
    similar_objects_groups = list(nx.connected_components(g))
    similar_objects_pairs = [list(group) for group in similar_objects_groups]

    # Mark instances for deletion and build a dict, image -> instances ids to be deleted
    for_deletion = []
    for group in similar_objects_pairs:
        keep_index = random.randint(0, len(group) - 1)
        for i, det in enumerate(group):
            if i != keep_index:
                for_deletion.append(det)

    for_deletion_dict = {}
    for item in for_deletion:
        image_name, det_id = item
        if for_deletion_dict.get(image_name, None):
            for_deletion_dict[image_name].append(det_id)
        else:
            for_deletion_dict[image_name] = [det_id]

    # Delete the duplicate annotations from the annotation dict
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

    logger.info("Exporting detections to GCP...")

    no_duplicates_annotations_filename = (
        f"{ann_name_base.split('.')[0]}_no_duplicates_image.json"
    )

    if local_ann_file_path:
        output_filepath = f"{output_path}/{no_duplicates_annotations_filename}"
    else:
        output_filepath = no_duplicates_annotations_filename

    # Export dataset to COCO format
    dataset.as_coco(annotations_path=output_filepath)

    # Read annotation json
    with open(output_filepath, "r") as json_file:
        data_dict = json.load(json_file)

    # Upload to GCS
    upload_json_to_gcs(
        bucket, f"{annotations_path}/{no_duplicates_annotations_filename}", data_dict
    )

    # Remove unwanted files
    files_for_deletion = ["temp_annotations.json", "temp_duplicates.csv"]
    for file in files_for_deletion:
        if os.path.exists(file):
            os.remove(file)

    if not local_ann_file_path:
        os.remove(output_filepath)

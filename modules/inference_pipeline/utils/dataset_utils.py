from __future__ import annotations

from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple, Union

import numpy as np
from cloud_utils import download_json_gcs, read_image_from_gcs_opencv
from google.cloud.storage import Bucket
from supervision import BaseDataset, DetectionDataset
from supervision.dataset.formats.coco import (
    build_coco_class_index_mapping,
    classes_to_coco_categories,
    coco_annotations_to_detections,
    coco_categories_to_classes,
    detections_to_coco_annotations,
    group_coco_annotations_by_image_id,
)
from supervision.dataset.utils import (
    build_class_index_mapping,
    map_detections_class_id,
    merge_class_lists,
    train_test_split,
)
from supervision.detection.core import Detections
from supervision.utils.file import read_json_file, save_json_file
from supervision.utils.iterables import find_duplicates


def save_coco_annotations(
    dataset,
    annotation_path: str,
    min_image_area_percentage: float = 0.0,
    max_image_area_percentage: float = 1.0,
    approximation_percentage: float = 0.75,
) -> None:
    Path(annotation_path).parent.mkdir(parents=True, exist_ok=True)
    licenses = [
        {
            "id": 1,
            "url": "https://creativecommons.org/licenses/by/4.0/",
            "name": "CC BY 4.0",
        }
    ]

    coco_annotations = []
    coco_images = []
    coco_categories = classes_to_coco_categories(classes=dataset.classes)

    image_id, annotation_id = 1, 1
    for image_path, image, annotation in dataset:
        if dataset.image_read:
            image_height, image_width, _ = image.shape
        else:
            image_height, image_width, _ = dataset.fixed_image_size
        image_name = f"{Path(image_path).stem}{Path(image_path).suffix}"
        coco_image = {
            "id": image_id,
            "license": 1,
            "file_name": image_name,
            "height": image_height,
            "width": image_width,
            "date_captured": datetime.now().strftime("%m/%d/%Y,%H:%M:%S"),
        }

        coco_images.append(coco_image)
        coco_annotation, annotation_id = detections_to_coco_annotations(
            detections=annotation,
            image_id=image_id,
            annotation_id=annotation_id,
            min_image_area_percentage=min_image_area_percentage,
            max_image_area_percentage=max_image_area_percentage,
            approximation_percentage=approximation_percentage,
        )

        coco_annotations.extend(coco_annotation)
        image_id += 1

    annotation_dict = {
        "info": {},
        "licenses": licenses,
        "categories": coco_categories,
        "images": coco_images,
        "annotations": coco_annotations,
    }
    save_json_file(annotation_dict, file_path=annotation_path)


def load_coco_annotations(
    images_directory_path: str,
    annotations_path: str,
    force_masks: bool = False,
) -> Tuple[List[str], List[str], Dict[str, Detections]]:
    coco_data = read_json_file(file_path=annotations_path)
    classes = coco_categories_to_classes(coco_categories=coco_data["categories"])
    class_index_mapping = build_coco_class_index_mapping(
        coco_categories=coco_data["categories"], target_classes=classes
    )
    coco_images = coco_data["images"]
    coco_annotations_groups = group_coco_annotations_by_image_id(
        coco_annotations=coco_data["annotations"]
    )

    images = []
    annotations = {}

    for coco_image in coco_images:
        image_name, image_width, image_height = (
            coco_image["file_name"],
            coco_image["width"],
            coco_image["height"],
        )
        image_annotations = coco_annotations_groups.get(coco_image["id"], [])
        image_path = f"{images_directory_path}/{image_name}"

        annotation = coco_annotations_to_detections(
            image_annotations=image_annotations,
            resolution_wh=(image_width, image_height),
            with_masks=force_masks,
        )
        annotation = map_detections_class_id(
            source_to_target_mapping=class_index_mapping,
            detections=annotation,
        )

        images.append(image_path)
        annotations[image_path] = annotation

    return classes, images, annotations


class CloudDetectionDataset(BaseDataset):
    """
    Contains information about a detection dataset. Handles lazy image loading
    and annotation retrieval, dataset splitting, conversions into multiple
    formats.

    Attributes:
        classes (List[str]): List containing dataset class names.
        images (Union[List[str], Dict[str, np.ndarray]]):
            Accepts a list of image paths, or dictionaries of loaded cv2 images
            with paths as keys. If you pass a list of paths, the dataset will
            lazily load images on demand, which is much more memory-efficient.
        annotations (Dict[str, Detections]): Dictionary mapping
            image path to annotations. The dictionary keys
            match the keys in `images` or entries in the list of
            image paths.
    """

    def __init__(
        self,
        classes: List[str],
        images: List[str],
        annotations: Dict[str, Detections],
        bucket: Bucket,
        image_read: bool = False,
        fixed_image_size: Tuple[int, int, int] = (640, 640, 3),
    ) -> None:
        self.classes = classes

        self.bucket = bucket

        self.image_read = image_read
        self.fixed_image_size = fixed_image_size

        if set(images) != set(annotations):
            raise ValueError(
                "The keys of the images and annotations dictionaries must match."
            )
        self.annotations = annotations

        # Eliminate duplicates while preserving order
        self.image_paths = list(dict.fromkeys(images))

    def _get_image(self, image_path: str) -> np.ndarray:
        """Assumes that image is in dataset"""
        return read_image_from_gcs_opencv(self.bucket, image_path)

    def __len__(self) -> int:
        return len(self.image_paths)

    def __getitem__(self, i: int) -> Tuple[str, np.ndarray, Detections]:
        """
        Returns:
            Tuple[str, np.ndarray, Detections]: The image path, image data,
                and its corresponding annotation at index i.
        """
        image_path = self.image_paths[i]
        if self.image_read:
            image = self._get_image(image_path)
        else:
            image = np.zeros(self.fixed_image_size, dtype=np.uint8)
        annotation = self.annotations[image_path]
        return image_path, image, annotation

    def __iter__(self) -> Iterator[Tuple[str, np.ndarray, Detections]]:
        """
        Iterate over the images and annotations in the dataset.

        Yields:
            Iterator[Tuple[str, np.ndarray, Detections]]:
                An iterator that yields tuples containing the image path,
                the image data, and its corresponding annotation.
        """
        for i in range(len(self)):
            image_path, image, annotation = self[i]
            yield image_path, image, annotation

    def __eq__(self, other) -> bool:
        if not isinstance(other, CloudDetectionDataset):
            return False

        if set(self.classes) != set(other.classes):
            return False

        if self.image_paths != other.image_paths:
            return False

        if self.annotations != other.annotations:
            return False

        return True

    def split(
        self, split_ratio=0.8, random_state=None, shuffle: bool = True
    ) -> Tuple[CloudDetectionDataset, CloudDetectionDataset]:
        """
        Splits the dataset into two parts (training and testing)
            using the provided split_ratio.

        Args:
            split_ratio (float, optional): The ratio of the training
                set to the entire dataset.
            random_state (int, optional): The seed for the random number generator.
                This is used for reproducibility.
            shuffle (bool, optional): Whether to shuffle the data before splitting.

        Returns:
            Tuple[DetectionDataset, DetectionDataset]: A tuple containing
                the training and testing datasets.

        Examples:
            ```python
            import supervision as sv

            ds = sv.DetectionDataset(...)
            train_ds, test_ds = ds.split(split_ratio=0.7, random_state=42, shuffle=True)
            len(train_ds), len(test_ds)
            # (700, 300)
            ```
        """

        train_paths, test_paths = train_test_split(
            data=self.image_paths,
            train_ratio=split_ratio,
            random_state=random_state,
            shuffle=shuffle,
        )

        train_input = train_paths
        test_input = test_paths

        train_annotations = {path: self.annotations[path] for path in train_paths}
        test_annotations = {path: self.annotations[path] for path in test_paths}

        train_dataset = CloudDetectionDataset(
            classes=self.classes,
            images=train_input,
            annotations=train_annotations,
            bucket=self.bucket,
        )
        test_dataset = CloudDetectionDataset(
            classes=self.classes,
            images=test_input,
            annotations=test_annotations,
            bucket=self.bucket,
        )
        return train_dataset, test_dataset

    @classmethod
    def merge(
        cls, dataset_list: List[CloudDetectionDataset], bucket: Bucket
    ) -> CloudDetectionDataset:
        """
        Merge a list of `DetectionDataset` objects into a single
            `DetectionDataset` object.

        This method takes a list of `DetectionDataset` objects and combines
        their respective fields (`classes`, `images`,
        `annotations`) into a single `DetectionDataset` object.

        Args:
            dataset_list (List[DetectionDataset]): A list of `DetectionDataset`
                objects to merge.
            bucket (Bucket): A GCS bucket instance

        Returns:
            (DetectionDataset): A single `DetectionDataset` object containing
            the merged data from the input list.

        Examples:
            ```python
            import supervision as sv

            ds_1 = sv.DetectionDataset(...)
            len(ds_1)
            # 100
            ds_1.classes
            # ['dog', 'person']

            ds_2 = sv.DetectionDataset(...)
            len(ds_2)
            # 200
            ds_2.classes
            # ['cat']

            ds_merged = sv.DetectionDataset.merge([ds_1, ds_2])
            len(ds_merged)
            # 300
            ds_merged.classes
            # ['cat', 'dog', 'person']
            ```
        """

        image_paths = list(
            chain.from_iterable(dataset.image_paths for dataset in dataset_list)
        )
        image_paths_unique = list(dict.fromkeys(image_paths))
        if len(image_paths) != len(image_paths_unique):
            duplicates = find_duplicates(image_paths)
            raise ValueError(
                f"Image paths {duplicates} are not unique across datasets."
            )
        image_paths = image_paths_unique

        classes = merge_class_lists(
            class_lists=[dataset.classes for dataset in dataset_list]
        )

        annotations = {}
        for dataset in dataset_list:
            annotations.update(dataset.annotations)
        for dataset in dataset_list:
            class_index_mapping = build_class_index_mapping(
                source_classes=dataset.classes, target_classes=classes
            )
            for image_path in dataset.image_paths:
                annotations[image_path] = map_detections_class_id(
                    source_to_target_mapping=class_index_mapping,
                    detections=annotations[image_path],
                )

        return cls(
            classes=classes, images=image_paths, annotations=annotations, bucket=bucket
        )

    @classmethod
    def from_coco(
        cls,
        images_directory_path: str,
        annotations_path: str,
        force_masks: bool = False,
        bucket: Bucket = None,
    ) -> Union[CloudDetectionDataset, DetectionDataset]:
        """
        Creates a Dataset instance from COCO formatted data.

        Args:
            bucket (Bucket): A GCS Bucket instance
            images_directory_path (str): The path to the
                directory containing the images.
            annotations_path (str): The path to the json annotation files.
            force_masks (bool, optional): If True,
                forces masks to be loaded for all annotations,
                regardless of whether they are present.

        Returns:
            DetectionDataset: A DetectionDataset instance containing
                the loaded images and annotations.

        Examples:
            ```python
            import roboflow
            from roboflow import Roboflow
            import supervision as sv

            roboflow.login()
            rf = Roboflow()

            project = rf.workspace(WORKSPACE_ID).project(PROJECT_ID)
            dataset = project.version(PROJECT_VERSION).download("coco")

            ds = sv.DetectionDataset.from_coco(
                images_directory_path=f"{dataset.location}/train",
                annotations_path=f"{dataset.location}/train/_annotations.coco.json",
            )

            ds.classes
            # ['dog', 'person']
            ```
        """
        annotation_path_local = "temp_annotations.json"

        # Download the annotation file
        download_json_gcs(bucket, annotations_path, annotation_path_local)

        classes, images, annotations = load_coco_annotations(
            images_directory_path=images_directory_path,
            annotations_path=annotation_path_local,
            force_masks=force_masks,
        )
        return CloudDetectionDataset(
            classes=classes, images=images, annotations=annotations, bucket=bucket
        )

    def as_coco(
        self,
        annotations_path: Optional[str] = None,
        min_image_area_percentage: float = 0.0,
        max_image_area_percentage: float = 1.0,
        approximation_percentage: float = 0.0,
    ) -> None:
        """
        Exports the dataset to COCO format. This method saves the
        images and their corresponding annotations in COCO format.

        !!! tip

            The format of the mask is determined automatically based on its structure:

            - If a mask contains multiple disconnected components or holes, it will be
            saved using the Run-Length Encoding (RLE) format for efficient storage and
            processing.
            - If a mask consists of a single, contiguous region without any holes, it
            will be encoded as a polygon, preserving the outline of the object.

            This automatic selection ensures that the masks are stored in the most
            appropriate and space-efficient format, complying with COCO dataset
            standards.

        Args:
            images_directory_path (Optional[str]): The path to the directory
                where the images should be saved.
                If not provided, images will not be saved.
            annotations_path (Optional[str]): The path to COCO annotation file.
            min_image_area_percentage (float): The minimum percentage of
                detection area relative to
                the image area for a detection to be included.
                Argument is used only for segmentation datasets.
            max_image_area_percentage (float): The maximum percentage of
                detection area relative to
                the image area for a detection to be included.
                Argument is used only for segmentation datasets.
            approximation_percentage (float): The percentage of polygon points
                to be removed from the input polygon,
                in the range [0, 1). This is useful for simplifying the annotations.
                Argument is used only for segmentation datasets.
        """
        if annotations_path is not None:
            save_coco_annotations(
                dataset=self,
                annotation_path=annotations_path,
                min_image_area_percentage=min_image_area_percentage,
                max_image_area_percentage=max_image_area_percentage,
                approximation_percentage=approximation_percentage,
            )

from dataclasses import dataclass
from typing import List, Optional

from pydantic import Field


@dataclass
class ActionType:
    name: str


@dataclass
class ActiveSampling:
    enable: bool
    confidence_interval: Optional[List[float]]
    probability: Optional[List[float]]
    selected_classes: Optional[List[int]]
    annotation_filename: str
    split: str


@dataclass
class Predict(ActionType):
    model_name: str
    config_name: str
    store_predictions: bool
    batch_size: int
    agnostic_nms: bool
    agnostic_nms_thresh: float
    bbox_conf_thresh: float
    keep_only_cpgs: bool
    active_sampling: ActiveSampling


@dataclass
class Area:
    viz_path: str
    output_path: str
    images_path: str
    data_path: str
    predictions_path: str
    annotations_path: str
    polygon: Optional[List[List[float]]]
    name: str = Field(pattern=r"^([a-z]+)(/[a-z]+){0,3}$")


@dataclass
class Actions:
    predict: Optional[Predict]


@dataclass
class SetupConfig:

    # Cloud settings
    google_token: str
    google_secret: str
    bucket_name: str
    public_bucket_name: str
    project_id: str

    # Tokens
    mapbox_token: str

    # Paths
    database_path: str
    logs_path: str
    polygons_database_path: str
    buildings_database_path: str
    models_database_path: str
    training_database_path: str

    # Settings
    sv_name: str
    osm_name: str
    annotations_filename: str
    custom_polygon_filename: str
    force_compute_graph: bool
    window_split_area: List[float]
    window_overlap: int
    cloud_logger: bool
    viz: bool

    # Area
    area: Area

    # Inference pipeline actions
    inference: Actions

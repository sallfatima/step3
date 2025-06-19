from dataclasses import dataclass
from typing import List, Optional

from pydantic import Field


@dataclass
class ActionType:
    name: str


@dataclass
class ImageRemoval(ActionType):
    neighbor_distance_meters: int
    export_classes_name: List[str]
    save_duplicate_crops: bool
    duplicate_crops_dir: str
    from_roboflow: bool
    from_roboflow_character: str
    from_roboflow_positions: List[int]
    local_ann_file_path: Optional[str]
    viz: bool


@dataclass
class LocationRemoval(ActionType):
    class_name: str
    from_roboflow: bool
    local_ann_file_path: Optional[str]
    viz: bool


@dataclass
class Export(ActionType):
    output_path: str
    classes_to_export: List[str]
    annotation_filename_to_export: str
    shop_displacement: int
    public: bool
    results_name: str
    from_roboflow: bool
    from_roboflow_character: str
    from_roboflow_positions: List[int]


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
    export: Optional[Export]
    image_removal: Optional[ImageRemoval]
    location_removal: Optional[LocationRemoval]


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

    # Removal/export pipeline actions
    removal_export: Actions

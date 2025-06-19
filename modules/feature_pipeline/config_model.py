from dataclasses import dataclass
from typing import List, Optional

from pydantic import Field


@dataclass
class ActionType:
    name: str


@dataclass
class Build(ActionType):
    overpass_url: str
    network_type: str
    unconnected_components: bool
    contract_graph: bool
    output_graph: bool
    enrich: bool
    distance_between_points: int
    max_chunk_size_osm: int
    max_chunk_size_find: int
    resume_sv_find_from: int
    max_chunk_retrieve_location: int
    max_chunk_size_osm_to_graph: int
    big_edges_thresh: int
    max_workers_merge: int
    viz: bool


@dataclass
class Card(ActionType):
    viz: bool
    max_workers: int
    force_recompute: bool


@dataclass
class Retrieve(ActionType):
    viz: bool
    img_size: List[int]
    heading: Optional[int]
    fov: int
    pitch: int
    max_chunk_size: int
    public: bool


@dataclass
class UploadForAnnotation(ActionType):
    roboflow_api_key: Optional[str]
    roboflow_project_name: Optional[str]
    max_chunk_size: int
    filtered_images: Optional[List[str]]
    annotations_database_source: Optional[str]
    annotations_filename: str
    split: str
    tag_list: List[Optional[str]]
    upload_classes_name: List[Optional[str]]


@dataclass
class UploadFromAnnotation(ActionType):
    common_classes: List[str]
    local_sources: List[str]
    annotations_filename: str


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
    build: Optional[Build]
    card: Optional[Card]
    retrieve: Optional[Retrieve]
    upload_for_annotation: Optional[UploadForAnnotation]
    upload_from_annotation: Optional[UploadFromAnnotation]


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

    # Feature pipeline actions
    features: Actions

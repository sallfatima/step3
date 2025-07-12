from build_utils import get_available_sv, get_osm, osm_to_graph
from card_utils import build_card
from checks_utils import check_broader_area, check_polygon
from cloud_utils import clean_intermediate_files, get_bucket
from general_utils import enclosing_rectangle, split_window
from logging_utils import log_func
from merge_utils import merge_sub_windows
from retrieve_utils import retrieve_images
from upload_utils import upload_for_annotation, upload_from_annotation


class Pipeline:
    def __init__(self, cfg):

        # Get config dictionary
        self.cfg = cfg

        # List of sub-polygons of a region (from a pre-defined area)
        self.sub_polygons_df = None

        # Get cloud bucket
        self.bucket = get_bucket(cfg.bucket_name, cfg.project_id)

        # Checking validity of the area's polygon
        self.check_polygon()

        # Define enclosing window for defined polygon
        enclosing_window = enclosing_rectangle(self.cfg.area.polygon)

        # Split initial window in smaller squared sub-windows
        self.windows = split_window(
            enclosing_window, self.cfg.window_split_area, self.cfg.window_overlap
        )

    @log_func
    def check_broader_area(self):
        compute_graph = (
            check_broader_area(self.cfg, self.bucket) or self.cfg.force_compute_graph
        )

        return compute_graph

    @log_func
    def check_polygon(self):
        self.cfg.area.polygon, self.sub_polygons_df = check_polygon(
            self.cfg, self.bucket
        )

    @log_func
    def run(self):
        # Run each action from the actions dictionary
        actions = list(self.cfg.features.keys())
        for action in actions:
            action_func = getattr(self, action)
            action_func()

    @log_func
    def build(self):

        # Checks if the build and merge processes should take place
        # NOTE: only if current polygon is FULLY part of the broader polygon, the computation will not take place
        compute_graph = self.check_broader_area()

        if compute_graph:
            # Get OSM data
            get_osm(self.cfg, self.windows, self.bucket)

            # Convert OSM data to OSM graphs
            osm_to_graph(self.cfg, self.windows, self.bucket)

            # Get available SV locations and build SV graphs
            get_available_sv(self.cfg, self.windows, self.bucket)

            # Merge sub windows
            merge_sub_windows(self.cfg, self.windows, self.bucket)

            # Clean intermediate files
            clean_intermediate_files(self.cfg, self.bucket)

    @log_func
    def card(self):
        # Computes the knowledge card for the selected area
        build_card(self.cfg, self.bucket, self.sub_polygons_df)

    @log_func
    def retrieve(self):
        # Download images
        retrieve_images(self.cfg, self.bucket)

    @log_func
    def upload_for_annotation(self):
        # Upload images
        upload_for_annotation(self.cfg, self.bucket)

    @log_func
    def upload_from_annotation(self):
        upload_from_annotation(self.cfg, self.bucket)

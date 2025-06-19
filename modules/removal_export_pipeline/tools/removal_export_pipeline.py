from checks_utils import check_polygon
from cloud_utils import get_bucket
from duplicate_utils import run_removal
from export_utils import export_to_client
from general_utils import enclosing_rectangle
from location_duplicates_utils import run_removal_location
from logging_utils import log_func


class Pipeline:
    def __init__(self, cfg):

        # Get config dictionary
        self.cfg = cfg

        # List of sub-polygons of a region
        self.sub_polygons_df = None

        # Get cloud bucket
        self.bucket = get_bucket(cfg.bucket_name, cfg.project_id)

        # Checking validity of the area's polygon
        self.check_polygon()

        # Define enclosing window for defined polygon
        self.enclosing_window = enclosing_rectangle(self.cfg.area.polygon)

    @log_func
    def run(self):
        # Run each action from the actions dictionary
        actions = list(self.cfg.removal_export.keys())
        for action in actions:
            action_func = getattr(self, action)
            action_func()

    @log_func
    def image_removal(self):
        run_removal(self.cfg, self.bucket)

    @log_func
    def location_removal(self):
        run_removal_location(self.cfg, self.bucket, tuple(self.enclosing_window))

    @log_func
    def export(self):
        export_to_client(self.cfg, self.bucket)

    @log_func
    def check_polygon(self):
        self.cfg.area.polygon, self.sub_polygons_df = check_polygon(
            self.cfg, self.bucket
        )

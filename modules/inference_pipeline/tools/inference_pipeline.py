from checks_utils import check_polygon
from cloud_utils import get_bucket
from logging_utils import log_func
from prediction_utils import predict


class Pipeline:
    def __init__(self, cfg):

        # Get config dictionary
        self.cfg = cfg

        # Get cloud bucket
        self.bucket = get_bucket(cfg.bucket_name, cfg.project_id)

        # Checking validity of the area's polygon
        self.check_polygon()

    @log_func
    def run(self):
        # Run each action from the actions dictionary
        actions = list(self.cfg.inference.keys())
        for action in actions:
            action_func = getattr(self, action)
            action_func()

    @log_func
    def check_polygon(self):
        self.cfg.area.polygon, _ = check_polygon(self.cfg, self.bucket)

    @log_func
    def predict(self):
        predict(self.cfg, self.bucket)

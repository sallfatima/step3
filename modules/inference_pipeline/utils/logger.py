import logging.config

import google.cloud.logging as gcl
import yaml

# Get Google Client
logger = logging.getLogger("main_cloud")


def setup_cloud_logging(project_id: str, config_path: str) -> None:
    """Setup for Google Cloud Logging"""

    # Google Cloud Client
    client = gcl.Client(project=project_id)

    # Read configuration for logger
    with open(f"{config_path}/cloud_logger_config.yaml") as f:
        config_dict = yaml.safe_load(f)

    # Add client to handler and load config to main logging module
    config_dict["handlers"]["cloud"]["client"] = client
    logging.config.dictConfig(config_dict)

import os
import sys

# Get absolute paths for the current module
UTILS_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), "utils")

# Ensure these directories are in sys.path
if UTILS_PATH not in sys.path:
    sys.path.insert(0, UTILS_PATH)

import hydra
from config_model import SetupConfig
from dotenv import load_dotenv
from hydra.core.config_store import ConfigStore
from hydra.core.hydra_config import HydraConfig
from logger import logger, setup_cloud_logging
from omegaconf import OmegaConf

from tools.removal_export_pipeline import Pipeline

# Load environment variables
load_dotenv()

# Set a configuration instance and store it in the repository
cs = ConfigStore.instance()
cs.store(name="main", node=SetupConfig)

# Set config path
CONFIG_PATH = "../../conf"


@hydra.main(
    config_path=CONFIG_PATH, config_name="removal_export_config", version_base="1.3"
)
def main(cfg: SetupConfig) -> None:

    # Setup logger
    if cfg.cloud_logger:
        setup_cloud_logging(cfg.project_id, CONFIG_PATH)

    # Log info
    logger.info(
        f"Running job named {HydraConfig.get().job.name}"
        f" with config: \n\n{OmegaConf.to_yaml(cfg=cfg, resolve=True)} \n"
    )

    # Instantiate Pipeline object and run
    pipeline = Pipeline(cfg)
    pipeline.run()


if __name__ == "__main__":
    main()

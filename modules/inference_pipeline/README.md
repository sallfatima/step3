# Inference Pipeline

This is the inference pipeline. It requires a GPU. Therefore, you need to spin-up cloud resources to run it.

Don't forget to replace/fill the `credentials.json.example` and the `.env.example` files (also check `terraform` directory).

Make sure you understand all the configuration parameters, from all actions, under `conf/inference` pipeline configuration.

## 1. Actions

### 1.1 Predict

This action uses the best model, for a specific country, to predict bounding boxes on the images corresponding to an area.

1. Takes a `model_name` and a `model_config` as input, in the configuration files for this action (`conf/inference/predict.yaml`). The model is the best performing model, saved after training.
2. It can perform active sampling, at the same time the prediction runs, through the parameters of the `active_sampling` in the configuration file (see [4. Active sampling](#4-active-sampling)).
3. The annotations (predictions) will be saved in the `database/annotations`, at the corresponding area path (see [storage and databses](../../README.md#-4-storage-and-databases)), under the `annotations_filename` defined in `conf/general/settings.yaml`. If this file already exists, no overwrite is allowed, and the name should be changed.

## 2. Area definition

Please revisit the [area definition](../feature_pipeline/README.md#2-area-definition) in the feature pipeline, if not already done so.

## 3. Model configuration

Configuration files for the model can be found under `network_configs/` directory. There is currently only one configuration file (`model_config.py`), which is custom-built, from scratch, starting from the configuration of the model used in the original [Co-DETR repo](https://github.com/Sense-X/Co-DETR/tree/main). You can find the original configuration file [here](https://github.com/Sense-X/Co-DETR/blob/main/projects/configs/co_dino_vit/co_dino_5scale_vit_large_coco.py).

The format of the configuration files follow the [MMDetection format](https://mmdetection.readthedocs.io/en/dev-3.x/user_guides/config.html) (**NOTE:** pay attention that the current model uses MMDetection 2.25.3, and not the latest MMDetection 3.X, thus the current configuration specification might differ a bit from this latest version, in case you want to modify it).

## 4. Active sampling

Active sampling can be activated, for an area, together with the prediction process. Here are the parameters that can be set for this process.

```
  active_sampling:
    # Whether to perform active sampling or not
    enable: false
    # [min, max] confidence for keeping detections with confidence in this interval
    confidence_interval: [0.2, 0.4]
    # Percentage (probability, at inference time) of samples to be persisted, per class. (NOTE: 0.5 represents 0.5%, not 50%)
    probability: [20, 10, 10]
    # Which classes to consider for sampling. Choose from: [cpg-corner-shop, corner-shop, street-booth-vendor]
    selected_classes: [0, 1, 2]
    # Annotation name:
    annotation_filename: "annotations_active_sampling.json"
    # Split name ('train', 'val', 'test')
    split: 'train'
```

If enabled, an annotation file (`annotation_filename`) with the sampled results, will be saved in the training annotations database, under the specified `split` (see [storage and databses](../../README.md#-4-storage-and-databases)).

NOTE: the `probability` and the `selected_classes` length must match (verified through code).
## 5. Installation

Locally, if GPU available:

TODO

## 6. Usage

### 6.1 Locally (if GPU available)

TODO

### 6.2 Google Cloud Platform

To initialize Terraform, run (**only once, at the project setup**):
```
make init
```
Then, **for each run**, you need to check and edit the `terraform/terraform.auto.vars` file. This file contains the CLI arguments for running the process.

Example of `terraform.auto.vars` file content:
```
cli_args_per_job = {
    jakarata = ["area.name=asia/indonesia/jakarta/demo_custom", "inference=[predict]", "inference.predict.active_sampling.enable=true"],
    mumbai = ["area.name=asia/india/mumbai/demo_custom", "inference=[predict]"],
}
```

In this example, active sampling is enabled for Jakarta (by default, false, in the config), and uses the pre-defined active sampling parameters in the config (those can be overwritten too, though). You can see the power of Hydra's parameter overwriting.


**IMPORTANT NOTE 1:** See how you can define multiple runs at the same time. Cloud resources will be allocated, and executions will happen, in parallel, for multiple areas, depending on the CLI arguments. **The name of a run (``dakar`` and ``lagos``, here) is a dummy, unique value, and it MUST CONTAIN ONLY CAPITAL LETTERS AND '-' character. Also, there should be no spaces in any of the strings from the list.**

Then, **to actually build the Docker and push it to the Artifact Registry**, and also check what Terraform will do, run:
```
make plan
```

**IMPORTANT NOTE 2:** Currently, the build + push of the Docker container needs to be done locally, as the image size is too big to be done on GitHub's VM, at a code push (see [TODOs](#7-todos)). Therefore, the `make plan` command needs to be run whenever code modifications are done, for this pipeline. If only the variables in `terraform.auto.vars` are modified, there is no need to run this command.

Then, to actually deploy and run the pipeline, run:
```
make apply
```

**IMPORTANT NOTE 1:** First, you need to make sure that the last push, and the associated GitHub actions were successful (see [TODOs](#7-todos)).

**IMPORTANT NOTE 2:** After a **successful** job run, the Batch Job (together with GPU resources) will be automatically destroyed, through a Pub/Sub system, that triggers a Cloud Build action, when a specific message is received.

You can destroy/clean all the other resources spawned, by running:
```
make destroy
```

### 6.3 Cloud Logging
 You can check the logs by inspecting the Google Cloud Logging Console. In `conf/cloud_logger_config.yaml` you can set the `handlers.cloud.name` variable to your name (currently set to `mihai`). By using this value in **All log names** filter, you will see only the logs related to your project.

## 7. TODOs
- [ ] Integrate a `.sh` script that will run as a dependency on the `apply` action, in Makefile (as the ``load_env.sh`` script), to verify that the last GitHub Action is completed successfully (before ``apply`` takes place).
- [ ] Reduce Docker image size and integrate the build + push in the CI/CD, at code push. Get rid of `make plan` command necessity (and the `build_push.sh` script behind it).

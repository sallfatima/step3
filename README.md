# :world_map: :shopping_cart: Geo-mapping

Geo-mapping stores across the world.

## :gear: 1. Setup
- Install [Poetry](https://python-poetry.org/docs/).
- Install [gcloud CLI](https://cloud.google.com/sdk/docs/install).
- Install [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli).
- Install [Docker](https://docs.docker.com/get-docker/).
- Create an account on [Mapbox](https://www.mapbox.com/). Go to ```Tokens``` tab and get the public token.
- Create an account on [Roboflow](https://roboflow.com/). After logging in, join the current geo-mapping project (lengoai_nigeria_lagos_apapa_od) by clicking this [link](https://app.roboflow.com/join/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ3b3Jrc3BhY2VJZCI6ImRkNVZCNnpsV0ZWbGRMNU9Ydk56WWN1ekVqYjIiLCJyb2xlIjoicmV2aWV3ZXIiLCJpbnZpdGVyIjoic2hhamFoYW5Ad2lzZXBsLmNvbSIsImlhdCI6MTczODMyMDkxN30.btEV5zkOu_Aksq5Q8HahhklzSKhFhB-qi7es3QIcGYA). Go to ```Settings -> API Keys -> Private API Key``` to get the Roboflow key.
- For GoogleAPI token follow [these](https://developers.google.com/maps/documentation/javascript/get-api-key) instructions.
- For a URL signing secret follow [these](https://developers.google.com/maps/documentation/maps-static/digital-signature) instructions. This si used to signthe URL when using Static StreetView API in the [`retrieve` action](modules/feature_pipeline/README.md/#1-actions), for downloading multiple images from Google Maps.
- If working on Windows machine: install Git Bash (from [git](https://git-scm.com/downloads) official download) and always run the `make` commands only from Git Bash.
- Generate a JSON key (a `.json` file) corresponding to the GCP Service Account that you are using (you should use `cloud-run` Service Account as it has all the necessary permissions). Follow section `Create a service account key -> Console` from [here](https://cloud.google.com/iam/docs/keys-create-delete#iam-service-account-keys-create-console).
- Replace all `credentials.json.example` files with the `.json` key previously generated (tip: you can rename it to `credentials.json`)
- For Google Cloud credentials run:
```shell script
gcloud auth application-default login
```
- For pre-commit hooks run:
```shell script
pre-commit install
```
- Replace all the `.env.example` files with `.env` files **in which all secrets / keys / tokens are filled.**


## :eyes: 2. Project overview

![Alt text](assets/architecture.png)

### 2.1 Modules and workflow
The project consists of 4 big modules (see also the diagram below).
Each module consists of one or more actions, that can be run separately for each module, as follows:

1. [Features module](modules/feature_pipeline/README.md) (input + yellow blocks) with following actions:
   1. `build`: Build OSM points graph and available Google Street View (GSV) points graph
   2. `card`: Build the "card" for an area (Mapbox map with statistics of images' dates and GSV coverage) -> **REQUIRES** `build` action to be run
   3. `retrieve`: Retrieve images corresponding to the points in graph -> **REQUIRES** `build` action to be run
   4. `upload_for_annotation`: Uploads annotations/predictions to Roboflow
   5. `upload_from_annotation`: Uploads locally downloaded, reviewed annotations, from Roboflow, to GCS (Google Cloud Storage)
2. [Inference module](modules/inference_pipeline/README.md) (blue block) with following actions:
   1. `predict`: Predict bounding boxes with the current best model -> **REQUIRES** `retrieve` action to be run
3. [Duplicate removal and export module](modules/removal_export_pipeline/README.md) (red blocks + output) with following actions:
   1. `image_removal`: Run removal pipeline based on images features -> **REQUIRES** `predict` action to be run
   2. `location_removal`: Run removal pipeline based on stores location and buildings footprints -> **REQUIRES** `image_removal` action to be run
   3. `export`: Export results in a format suitable for client deliverables
4. [Training module](modules/training_pipeline/README.md) (not present in the diagram) with following actions:
   1. **IMPORTANT NOTE:** The training module is not implemented in the project yet. The notebook used to train the model in Vertex AI Workbench, and additional files are provided, though.

**IMPORTANT NOTE:** We recommend reading this README first, and then the READMEs of each pipeline, in the order presented above.

**Example of commands WORKFLOW for Lagos, Nigeria**:

1. Run ``build``, ``card``, ``retrieve`` steps from the feature pipeline

```
cli_args_per_job = {
    lagos = ["area.name=africa/nigeria/lagos", "features=[build,card,retrieve]"],
}
```
2. Run ``predict`` action from inference pipeline

```
cli_args_per_job = {
    lagos = ["area.name=africa/nigeria/lagos", "inference=[predict]"],
}
```

3. Run ``image_removal`` and `location_removal` actions from removal_export pipeline

```
cli_args_per_job = {
    lagos = ["area.name=africa/nigeria/lagos", "removal_export=[image_removal,location_removal]"],
}
```

4. Run ``upload_for_annotation`` action from feature pipeline, to export to Roboflow Annotation

```
cli_args_per_job = {
    lagos = ["area.name=africa/nigeria/lagos", "features=[upload_for_annotation]"],
}
```

5. Download locally annotated zip from Roboflow, in COCO-MMDetection format, by exporting the newly created dataset.

6. Run ``upload_for_annotation`` action from feature pipeline, to upload the reviewed annotation back to GCS

```
cli_args_per_job = {
    lagos = ["area.name=africa/nigeria/lagos", "features=[upload_from_annotation]"],
}
```

7. Run ``export`` action from removal_export pipeline, to export in client deliverables format.

```
cli_args_per_job = {
    lagos = ["area.name=africa/nigeria/lagos", "removal_export=[export]"],
}
```


### 2.2 Tree structure

```
root
│── assets                          <- assets used in the README
│── conf                            <- config files (see section 3)
│ └── ...
│── modules
│ ├── feature_pipeline
│ │ ├── osm_utils                         <- utils for building OSM graph
│ │ │ └── ...
│ │ ├── scripts                           <- scripts used in Makefile
│ │ │ └── ...
│ │ ├── terraform                         <- terraform deployment
│ │ │ ├── main.tf                            <- HCL definition for terraform deployment
│ │ │ ├── terraform.auto.tfvars              <- ENTRYPOINT for defining deployment arguments
│ │ │ └── variables.tf                       <- variables used in terraform HCL
│ │ ├── tools
│ │ │ └── ferature_pipeline.py               <- main logic for the pipeline, based on actions
│ │ ├── utils                             <- utils for the feature pipeline
│ │ │ └── ...
│ │ ├── Dockerfile
│ │ ├── Makefile
│ │ ├── config_model.py                   <- pydantic models for all config parameters of the module
│ │ ├── main.py                           <- ENTRYPOINT for running the pipeline
│ │ └── ....
│ ├── inference_pipeline
│ │ ├── network_configs                   <- directory for MMDet style network config
│ │ │ └── model_config.py                    <- entire config for inference
│ │ ├── scripts
│ │ │ └── ...
│ │ ├── terraform
│ │ │ ├── main.tf
│ │ │ ├── terraform.auto.tfvars
│ │ │ └── variables.tf
│ │ ├── tools
│ │ │ └── inference_pipeline.py
│ │ ├── utils
│ │ │ └── ...
│ │ ├── Dockerfile
│ │ ├── Makefile
│ │ ├── config_model.py
│ │ ├── main.py
│ │ └── ....
│ ├── removal_export_pipeline
│ │ ├── image_matching                    <- utils for image matching algorithms (based on https://github.com/Vincentqyw/image-matching-webui)
│ │ │ └── ...
│ │ ├── scripts
│ │ │ └── ...
│ │ ├── terraform
│ │ │ ├── main.tf
│ │ │ ├── terraform.auto.tfvars
│ │ │ └── variables.tf
│ │ ├── tools
│ │ │ └── removal_export_pipeline.py
│ │ ├── utils
│ │ │ └── ...
│ │ ├── Dockerfile
│ │ ├── Makefile
│ │ ├── config_model.py
│ │ ├── main.py
│ │ └── ....
│ └── training_pipeline
│   ├── Co_DETR.ipynb                     <- notebook used in training on VertexAI Workbench (contains data retreival and trai/eval command)
│   ├── model_config.py                   <- entire config for training
│   └── coco.py                           <- script for being able to run classwise mAP
└── tools                         <- different tools used in the project
  ├── duplicate_review.py           <- prepare data for duplicate review
  ├── product_details.py            <- retrieve prodiuct details form photos using LLMs (not related to geo-mapping)
  ├── regions_display.py            <- display all regions (polygons) from pre-defined databases for Senegal and Nigeria
  └── split_area.py                 <- split an input area into multiple sub-areas (for parallel computation, if necessary)
```


The pipelines are pretty similar in structure. Therefore, all the directories/files for the `feature_pipeline` are detailed. For the other pipelines, only different modules from `feature_pipeline` are detailed.

## :page_with_curl: 3. Configuration files

[Hydra](https://hydra.cc/docs/intro/) configuration management is used. You should be familiar with how Hydra CLI parameter specification system work (to understand the `Usage` sections from each pipeline).
 All configuration files are under `conf` directory, in the following structure:
```
conf
│── features                        <- configs for features associated actions
│ ├── build.yaml
│ ├── card.yaml
│ ├── retrieve.yaml
│ ├── upload_for_annotation.yaml
│ └── upload_from_annotation.yaml
│── general                         <- configs for general usage (used in multiple modules)
│ ├── area.yaml
│ ├── cloud.yaml
│ ├── paths.yaml
│ ├── settings.yaml
│ └── tokens.yaml
│── inference                       <- configs for inference associated actions
│ └── predict.yaml
│── removal_export                  <- configs for duplicate removal and export associated actions
│ ├── export.yaml
│ ├── image_removal.yaml
│ └── location_removal.yaml
│── cloud_logger_config.yaml        <- cloud logger module config
│── features_config.yaml            <- features module config (composition of multiple configs)
│── hydra_config.yaml               <- hydra core config
│── inference_config.yaml           <- inference module config (composition of multiple configs)
└── removal_export_config.yaml      <- duplicate removal end export module config (composition of multiple configs)
```

We provide a description of each config variable in the `.yaml` files.

**IMPORTANT NOTE:** You should be aware of all the config variables, which should be changed, which not, and how they affect the code.

## 🛢️ 4. Storage and databases
GCS is used for storing everything.

The usual files structure is: `CONTINENT -> COUNTRY -> CITY -> REGION -> file`. `REGION` can be either a city region, a neighborhood, or even a couple of streets. This file structure is common to almost all "subdirectories" in a bucket.

From now on, we will refer to an `area` as any of `CONTINENT/COUNTRY/CITY/REGION`.

Storage structure (according to current config files values):

```
lengo-geomapping                                   <- private bucket
│── buildings_database                                <- stores files containing all buildings for a country based on a subset of OpenBuildings Dataset
│ └── africa
│   ├── senegal
│   │ └── open_buildings_v3_polygons_ne_110m.csv.gz            <- buildings footprints (used in location based duplicate removal)
│   └── nigeria
│     └── open_buildings_v3_polygons_ne_110m.csv.gz            <- buildings footprints (used in location based duplicate removal)
│── cloud_build_logs                               <- stores automatic logs created from cloud build (used for cloud functions)
│── database                                       <- stores graphs, images, custom polygons, predictions, annotations, areas' cards
│   └── see section 4.1
│── models_database                                <- stores best models
│ ├── ViT_pretrained_objects365_coco.pth              <- model used as initializer when training the network (fine-tuning starting point) for any country
│ └── africa
│   ├── senegal
│   │ └── best_model_coder.pth                              <- best model for the country
│   └── nigeria
│     └── best_model_codetr.pth                             <- best model for the country
│── polygons_database                              <- stores pre-defined administrative poygon databases (taken from the internet)
│ └── africa
│   ├── senegal
│   │ ├── senegal_map.html                                  <- displays multiple levels polygons for nigeria
│   │ ├── polygons                                          <- stores multiple levels polygons for senegal
│   │ └── dakar_lengo                                       <- stores multiple levels polygons for dakar, as defined by Lengo
│   │    └── ...
│   └── nigeria
│     ├── nigeria_map.html                                  <- displays multiple levels polygons for nigeria
│     └── polygons                                          <- stores multiple levels polygons for nigeria
│        └── ...
└── training_annotations_database                  <- stores annotation files used in training
  └── africa
    └── nigeria
      ├── base_dataset                                      <- last version of annotations stored in Roboflow
      │  └── annotations
      │     ├── train
      │     │  └── _annotations.coco.json                      <- train annotations in latest dataset
      │     ├── val
      │     │  └── _annotations.coco.json                      <- val annotations in latest dataset
      │     └── test
      │        └── _annotations.coco.json                      <- test annotations in latest dataset
      ├── lagos                                             <- city level
      │  ├── adekunle-ayietoro                                 <- region level
      │  │  └── annotations                                       <- annotations at region level
      │  │     ├── train
      │  │     │  └── annotations_active_sampling.json               <- annotations coming from active sampling
      │  │     ├── valid
      │  │     │  └── annotations_active_sampling.json               <- annotations coming from active sampling
      │  │     └── test
      │  │        └── annotations_active_sampling.json               <- annotations coming from active sampling
      │  ├── annotations                                       <- annotations at city level
      │  └── ...
      └── ...
```

**OBSERVATIONS:**
1. **buildings database**
   1. The buildings footprints for a country were obtained using [this](https://colab.research.google.com/github/google-research/google-research/blob/master/building_detection/open_buildings_download_region_polygons.ipynb) notebook.
2. **models_database**
   1. There is currently **ONLY ONE MODEL, for Nigeria**, that is replicated for Senegal too (just to make the pipeline work for senegalese areas). The models are not different for each of the countries. The model storage was designed per country, however, this might change in the future.
3. **polygons database**
   1. Pre-defined polygons for Nigeria were obtained from [here](https://data.humdata.org/dataset/cod-ab-nga)
   2. Pre-defined polygons for Senegal were obtained from [here](https://hgl.lib.harvard.edu/catalog/stanford-qz495ps5072).
   3. Pre-defined polygons for Dakar Lengo were obtained from [here](https://drive.google.com/drive/folders/1DHehqSNqdFBZTBaPj0Vmr8OtH8_-4klv).
4. **training_annotations_database**
   1. The annotations files are in COCO-MMDetection format (exported as such, from Roboflow, and compatible with our model input requirements).
   2. The annotations in the `training_annotations_database` are storing image **filenames** (not paths), in the `.json`, COCO-like files. However, they are linked through code, to point to the images at the corresponding paths in `images` directories from `database`, for each area. This way, different annotations can be linked to the same images (if necessary).
   3. The `base_dataset` in `training_annotations_database` contains all the annotations of the **current dataset used to train our latest model**, and corresponds to **dataset version 34 in Roboflow**. The images for this version are stored at the corresponding path in `database` directory (`database/africa/nigeria/base_dataset/images/`)
   4. Besides `base_dataset` annotations, that were already reviewed by the annotators, **all the other annotations come from active sampling** used in the inference pipeline. However, these annotation files should be first reviewed by annotators, before properly used in the training. Current active sampling results, were not reviewed, and were stored just for testing purposes. These other annotations (besides `base_dataset` ones) are stored like this to keep track of the location where they come from.
5. **database**
   1. **see section 4.1**


### 4.1 Database

The files in `database` have a similar structure as the other databases, and follow this template:

```
database
├─── <CONTINENT>
│   ├─── <COUNTRY>
│   │   ├─── <CITY>
│   │   │   ├─── <REGION>
│   │   │   │    └─── output_directories
│   │   │   └─── output_directories
│   │   └─── output_directories
│   └─── output_directories
└─── output_directories
```

- `output_directories` are a suite of multiple directories (not all, necessarily) for an area:
  - `viz` -> stores plots, figures and duplicated crops from the image based removal pipeline (under `duplicate_crops` directory)
  - `output` -> stores intermediary output, while building and merging the graph (directory deleted when process is complete)
  - `data` -> stores OSM + StreetView graphs, and the card (coverage and dates statistic) for an area
  - `annotations` -> stores multiple (intermediate) annotation files (e.g.: after predict action, after image based removal, after location based removal, OR, other annotation files)
  - `images` -> stores all images gathered in this area
  - `deliverables` -> stores CSV and Excel with the annotated data, suitable to be uploaded on maps, or deliver them to the clients
  - `images_annotated` -> stores images with predictions/annotations drawn on them, if private link to PhotoURL is created (otherwise these images will be stored in a public bucket, at a similar path)



## :computer: 5. Deployment, CI/CD, Makefile

Docker containers are employed for each of the pipelines. They are deployed, through Terraform, on Google Cloud Platform, either using Cloud Scheduler Jobs that does not require a GPU / inference steps (feature pipeline), or through Batch Jobs (activated by Google Cloud Scheduler Jobs), for the pipelines that require a GPU (inference + removal_export pipelines).

For the feature pipeline, the entire CI/CD is employed by GitHub Actions (see `workflows/feature_pipeline.yaml`). This includes: checkout, Google Cloud Authentication, Google Cloud SDK Installation, Google Artifact Registry creation/setup, Docker Authentication, and Docker build and push to Artifact Registry). The Google Authentication is realised through [Workload Identity Pool (WIP)](https://cloud.google.com/iam/docs/manage-workload-identity-pools-providers), by using a WIP secret and a Service Account secret stored in the GitHub repository Secrets.

For the inference and removal_export pipelines, all the steps for the CI/CD, are managed in the `scripts/build_push.sh` scripts for each pipeline, because the Docker image sizes are too big to be built directly on the GitHub's VMs (see TODOs, to reduce image size), and they need to be built locally. The Google Authentication is done through Service Account Impersonation, and the access token generated is used to log in into Docker. These `build_push.sh` scripts are invoked when running the `plan` action in the Makefile (using `make plan` command). As for Terraform, the `terraform plan` command is recommended to be run before `terraform apply` command, We have built a similar system using `make` commands in ht Makefile of each pipeline.

Each pipeline has an associated Makefile with similar commands to Terraform ones (`init`, `plan`, `apply`, `destroy`). Each command will run Terraform basic commands, but also other scripts like `scripts/load_env.sh` (exports secret environment variables to terraform secret `.tfvars` files), or `scripts/build_push.sh`, depending on the pipeline.

## :wrench: 6. Other tools

Other necessary tools are stored under `tools/` directory. Here is a breakdown:

TODO

## :white_check_mark: 7. TODOs

You should also check the TODOs sections of each individual pipeline. Below you will find TODOs that apply to multiple (common) parts of the project:

1. **Workflow**:
   1. [ ]  Fix the cloud function email notification for Batch Job failures, in the inference and duplicate removal pipelines (maybe also add email notification for failure in the feature pipeline, too, although expensive resources are not used, like a GPU).
   2. [ ]  Investigate the resource needed for inference and duplicate removal pipelines, and allocate the right (minimum required) resource for Batch Jobs (currently a bit too loose I believe, in terms of boot disk memory; the GPU is the minimum possible, a T4)
   3. [ ]  Create a spatial database, such that for an area (a polygon) that encompasses another smaller, already processed area (polygon), the data (images, or card, or predictions, or whatever) is automatically retrieved, and not computed again, from scratch. THIS MAY INVOLVE A LOT OF WORK, AND ENTIRE CODE REFACTORING :(

2. **Code**:
   1. [x]  Write READMEs for the entire project.
   2. [ ]  Factorize `check_utils`, `cloud_utils`, `dataset_utils`, `logger`, `logging_utils`, `polygon reader_utils`, `polygon_readers` from the pipelines (they are implemented in multiple pipelines, duplicated code) - move them in a common `utils` directory.
   3. [ ]  Clean the code (add docstrings, typing, refactoring, wherever necessary).

## 	:adult: 6. Contributors
Mihai David - [davidmihai9805@gmail.com](mailto:davidmihai9805@gmail.com).

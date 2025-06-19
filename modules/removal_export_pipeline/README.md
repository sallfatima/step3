# Duplicate removal and export pipeline

This is the duplicate removal and export pipeline. It requires a GPU. Therefore, you need to spin-up cloud resources to run it (or you can use your local GPU, if you have one).

Don't forget to replace/fill the `credentials.json.example` and the `.env.example` files (also check `terraform` directory).

Make sure you understand all the configuration parameters, from all actions, under `conf/removal_export` pipeline configuration.

## 1. Actions

### 1.1 Image removal

This must be the first action when starting the duplicate_removal pipeline.

1. Takes as input the images with detections found in the inference pipeline (``predict`` action).
2. Keeps only the images which contain classes of interest, according to configuration file.
3. Creates geographical "balls", of given radius, around each image lat/lon location (considered as the centers of the "balls").
4. For each image creates a list of image neighbours (based on the already computed "balls").
5. Creates list of unique image pairs from the neighboring lists.
6. For each image pair, and for each detection class:
   1. Creates list of all possible combinations (pairs) of detections in the images.
   2. The detections (bounding boxes) are cropped from the images, thus combinations of cropped bounding boxes will be created.
   3. Runs each detection pair through multiple image matching APIs. Based on the number of matches found, and the configured thresholds, each API will consider a pair of images as duplicates, or not. Please check section [Image Matching](#2-image-matching).
   4. Based on a majority voting (currently 2/3), select pair as duplicate.
   5. Creates list of duplicate detection pairs and saves it in a temporary file.
7. Creates graph, in which a connected components is a group of images detections that were similar to each other
   1. Let's say image A has detections A1, A2, and image B has detections B1, B2, and image C has detections C1, C2. If A1, B1, and C2 were capturing the same shop, thus duplicates, they will represent a connected component in the graph.
8. Keeps only one sample at random from each connected component, and remove the others
9. If an image happens to have no more detections in it, it is also removed.
10. Recreates the dataset with the remaining images and detections and saves it in GCS.


### 1.2 Location removal

TODO

### 1.3 Export

TODO

## 2. Image matching

This pipeline uses code from [this](https://github.com/Vincentqyw/image-matching-webui) repo to build and configure the Image Matching APIs.

You can also play in their [Hugging Face Space](https://huggingface.co/spaces/Realcat/image-matching-webui).

TODO

## 3. TODOs
- [ ]  Inspect correctness of graph based removal logic in image removal pipeline.
- [ ]  Integrate a `.sh` script that will run as a dependency on the `apply` action, in Makefile (as the ``load_env.sh`` script), to verify that the last GitHub Action is completed successfully (before ``apply`` takes place).
- [ ]  Check/modify the loading of entire OpenBuildings dataset, for a country, or improve loading and filtering, when estimating location, in the location based removal pipeline (`utils/location_estimator/buildingmanager.py`).
- [ ]  Play with the ``min_distance_between_shops`` parameter in aggregate function from `utils/location_estimator/aggregate.py` script (and/or make it configurable parameter).
- [ ]  Improve speed for image based removal pipeline by running the feature matching APIs in parallel (or on threads).
- [ ]  Check image based removal error (OpenCV one) in `duplicate_utils.py` and fix it - currently patched to only pass, but it can improve the accuracy of the algorithm (**ERROR**: [duplicate_utils|L316] 2025-02-17T15:34:47+0000: Exception: OpenCV(4.10.0) /io/opencv/modules/calib3d/src/usac/estimator.cpp:353: error: (-215:Assertion failed) !model.empty() in function 'setModelParameters')
- [ ]  Tune the hyperparameters (``max_threshold`` and ``max_keypoiunts`` in the 'matcher' dict of each model CONF -> `utils/image_matching_conf.py`) of the current APIs, or add more APIs.

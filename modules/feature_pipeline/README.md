# Feature Pipeline

This is the feature pipeline. It does not require a GPU.

Don't forget to replace/fill the `credentials.json.example` and the `.env.example` files (also check `terraform` directory).

Make sure you understand all the configuration parameters, from all actions, under `conf/features` pipeline configuration.

## 1. Actions

### 1.1 Build

This must be the first action when starting off with a new area.

1. Splits a given area (polygon, or area name from pre-defined database) into square sub-windows of a specific surface area, e.g. 600x600 m^2 (NOTE: besides step 5, all steps are described for each sub-window):
2. Uses [this](https://github.com/AndGem/OsmToRoadGraph) repo to build the OpenStreetMap (OSM) road graph for the sub-window. Points over-sampling is applied to increase the number of acquired points along OSM roads (decrease the distance between sampled points along OSM roads).
3. Based on the points of the OSM graph, The Street View available location (SV graph) for Google Maps is built: for each point in the OSM graph StreetViewPanorma Service from Google Maps JS API, is used to query the closest Google Maps point with a SV panorama available, in a given maximum radius. Also, the links to the previous and next SV points are kept.
4. Based on the connections between the points in the graph, we compute the heading parameter needed to acquire the meta-data of photos from the left and right-hand sides of the road (from the panoramas at those locations), and query images information using Google's Python Static Street View API.
5. We merge results from all sub-windows (both in terms of graphs and image meta-data).
6. The OSM and StreetView graphs are stored in the GCS, under `database/<area_path>/data/`.

### 1.2 Card

This action requires the ``build`` action to be finished.

1. From the build phase, we obtain each image acquisition date (from the response of the Static SV API), the total length of the roads (OSM graph), and the total length of Street View covered roads (SV graph).
2. We compute the 'card' of an area, that encompasses the SV coverage percentage and the images' dates distribution across multiple years.
3. The cards are stored in GCS, under `database/<area_path>/data/card_main.html` (and `database/<area_path>/data/card_secondary.html`, if necessary -> see[]()).

### 1.3 Retrieve

This action requires the ``build`` action to be finished.

1. We use Static Street View API to also retrieve the images at the specific locations (not only request for meta-data). This is costly, as it requires downloading the SV images themselves (NOTE: for retrieving images we use the maximum allowed size, 640x640 and a FOV (zoom) of 95).
2. Images are stored in GCS. The filename format, for an image is: ``<lat>_<lon>_<heading_index>_<side_index>_<heading>_<fov>_<date>.jpg`` NOTE: In an intersection, multiple images are taken from very similar spots, as the Google car passes the intersection from multiple directions (headings), thus the ``heading_index``. The `side_index` refers to the left/right sides of the road. The `heading`, `<fov>` and `date` are parameters of the image, returned by the Static Street View API.

### 1.4 Upload for annotation

This action requires an annotations file to be present in the GCS. For this, at least the inference pipeline (``predict`` action) should be run (though, RECOMMENDED to be run on results obtained from the duplicates removal actions: ``image_removal``, ``location_removal``)
1. A specific annotation file (together with the corresponding images) will be uploaded to Roboflow for inspection. Only selected classes will be uploaded. (Optionally) Only specific files from the same annotation file can be selected for upload.


### 1.5 Upload from annotation

This action requires a Roboflow annotations zip (images + annotation) to be present locally (exported form Roboflow, after inspection).

1. Uploads one (or more) annotation files to GCS. If more annotation files are provided, they are concatenated and saved under the same annotation file.


## 2. Area definition
**NOTE: VERY IMPORTANT FOR ALL OTHER PIPELINES**:

The area config is the file under `conf/general/area.yaml`. This contains the database paths related to an area, as well as 2 very important parameters: `name` and `polygon`.

### 2.1 Area name
**The area `name` needs to be specified as a parameter to any action of any pipeline**, such that the entire specific process is linked to this area. Hydra configuration management will raise an error if this is not specified (see the `???` values in the config, Hydra's specific value for mandatory input).

Pre-defined area names:
1. To check the available pre-defined areas names for a country, one can inspect the specific countries polygon map, on multiple levels, according to the [administrative levels](https://en.wikipedia.org/wiki/List_of_administrative_divisions_by_country). For example for Nigeria, this can be found at: `lengo-geomapping/polygons_database/africa/nigeria/nigeria_map.html`
2. An example for an area `name` definition in this case is: `africa/nigeria/lagos/osho` (NOTE: remember the format `continent/country/city/neighborhood`) -> you want to process the Osho neighborhood (region), in Lagos (city), from Nigeria (country), Africa (continent)
3. You can also process an entire pre-defined city, like this: `africa/senegal/dakar` -> Dakar (city), Senegal (country), Africa (continent)

Custom area:
1. If you do not want to use a pre-defined area name (and it's subsequent pre-defined ``polygon``), you must suffix your custom area name with `_custom`. **Note that the custom area name, from a level (region, city, or country) can be anything, as long as it is suffixed with `_custom`.**
2. An example is: `africa/nigeria/lagos/osho_custom`. This indicates that you want to process either the Osho area, but with other boundaries defined by you in the `polygon` attribute (see next section), either another smaller area in the Osho neighborhood, for which again, you need a custom `polygon` to be defined.
3. Another example is: `africa/nigeria/lagos/osho_unilever_streets_custom`. The meaning is the same as before, just that you used a custom name for the area too.

### 2.2 Area polygon
The area's `polygon` parameter can be specified, or not, depending on what area is chosen as input:
1. If a pre-defined area is used (meaning the name of that area, is part of the pre-defined administrative polygons, from the polygons database), the `polygon` value should be `null` in the config (default value), as the pre-defined polygon will be used.
2. If a custom area is used (area name is suffixed by `_custom`):
   1. **If used for the first time**: you need to specify the `polygon` attribute for this area, as a list of `[lat, lon]` coordinate pairs (type: `List[List[lat, lon]]`). This custom definition will be saved to the database.
   2. **If already used once in any of the pipelines**: You do not have to specify the `polygon` attribute because any other subsequent use of this custom area, will read the already saved polygon value from the db.


NOTE: For each pipeline (therefore, each action), the first step is to verify the integrity of the pair (`name`, `polygon`), and retrieve, or not, the pre-defined polygon in the database.

## 3. Installation

Locally:

```
cd modules/feature_pipeline
poetry install
```

This will create a local virtual environment, under the `feature_pipeline/.venv` directory.

## 4. Usage

### 4.1 Locally

**We do not recommend to run the pipeline locally**, especially for big areas, as they take a long time. However, this is how to run it:
```
cd modules/feature_pipeline
poetry run python main.py <arguments>
```

Example command for running the pipeline:

```
poetry run python main.py area.name=africa/senegal/dakar_lengo/colobane_hlm6_custom area.polygon=[[14.697903,-17.450442],[14.696844,-17.45142],[14.697903,-17.450442]] features=[build,card,retrieve]
```

The ``build``, ``card``, and ``retrieve`` actions will be run, sequentially, for `colobane_hlm6_custom` custom region (a custom polygon was defined as this is, virtually, the first time this custom area is used in any of the pipelines).
Observe that multiple actions can be piped together, sequentially, by specifying a list of actions for the `features` pipeline.

**IMPORTANT NOTE:** When calling a pipeline, is mandatory to select a list of actions for it (see `features` in `conf/features_config.yaml` OR `inference` in `conf/inference_config.yaml` OR `removal_export` in `conf/removal_export_config.yaml`)

### 4.2 Google Cloud Platform

To initialize Terraform, run (**only once, at the project setup**):
```
make init
```
Then, **for each run**, you need to check and edit the `terraform/terraform.auto.vars` file. This file contains the CLI arguments for running the process.

Example of `terraform.auto.vars` file content:
```
cli_args_per_job = {
    dakar = ["area.name=africa/senegal/dakar/plateau", "features=[upload_for_annotation]"],
    lagos = ["area.name=africa/nigeria/lagos", "features=[build,card,retrieve]"],
}
```
**IMPORTANT NOTE:** See how you can define multiple runs at the same time. Cloud resources will be allocated, and executions will happen, in parallel, for multiple areas, depending on the CLI arguments. **The name of a run (``dakar`` and ``lagos``, here) is a dummy, unique value, and it MUST CONTAIN ONLY CAPITAL LETTERS AND '-' character. Also, there should be no spaces in any of the strings from the list.**

Then, just to check what Terraform will do (not mandatory, but recommended), run:
```
make plan
```
Then, to actually deploy and run the pipeline, run:
```
make apply
```

**IMPORTANT NOTE:** First, you need to make sure that the last push, and the associated GitHub actions were successful (see [TODOs](#6-todos)).

After the job finishes you can destroy/clean the resources spawned, by running:
```
make destroy
```

### 4.3 Cloud Logging

 You can check the logs by inspecting the Google Cloud Logging Console. In `conf/cloud_logger_config.yaml` you can set the `handlers.cloud.name` variable to your name (currently set to `mihai`). By using this value in **All log names** filter, you will see only the logs related to your project.

## 5. Possible errors

During the ``build`` phase, you may encounter a fail of the pipeline (by checking the Cloud Run Jobs Console). This is because the maximum attempts for StreetView points finding, for a window, is reached (in function `get_available_sv_run` from `utils/build_utils.py`). This error usually occurs when multiple areas are ran in parallel (from the ``terraform.auto.tfvars`` file), because of the high number of requests to Google Maps JS API. Even though a retry mechanism with timeouts is in place, when performing a run for multiple areas in parallel, the error might occur.

**FIX:**

You should check the Cloud Logging and see what is the latest sub-window your pipeline reached in the SV graph building phase (e.g.: look for logs in the format: `SV file <area_name> -- [...] <current_window_index>/<max_window_index>`). **Pay attention that sub-windows are processed on threads and the last entry may not actually be the real last one**, so you should consider a`<current_window_index>`  a bit lower.

Then, for the specific area, you need add the following CLI, when you tun apply again, to resume the pipeline, from a specific sub-window:

If the error still persists, please leave only one area in the `terraform.auto.tfvars`.
```
features.build.resume_sv_find_from=<current_window_index>
```

Example of full command, if the last sub-window processed was 207:
```
cli_args_per_job = {
    lagos = ["area.name=africa/nigeria/lagos", "features=[build,card,retrieve]", features.build.resume_sv_find_from=200],
}
```

## 6. Polygon databases and card details

TODO

## 7. TODOs:
- [ ] Integrate a `.sh` script that will run as a dependency on the `apply` action, in Makefile (as the ``load_env.sh`` script), to verify that the last GitHub Action is completed successfully (before ``apply`` takes place).
- [ ] `upload_from_annotation` (in `upload_utils.py`): check the merge of the datasets. Currently, duplicate images paths (present in 2 or multiple annotation files) are not allowed (by default, `supervision` library throws an error if duplicate image paths are found). We would need to overwrite the initial (first) annotation file, with annotations from subsequent files (if any), where duplicated image paths exist (as those could come from the inspection process, with refined annotations).

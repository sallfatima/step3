from typing import List, Optional, Tuple

import geopandas as gpd
from cloud_utils import read_json_gcs, upload_json_to_gcs
from config_model import SetupConfig
from general_utils import reverse_lat_lon
from google.cloud.storage import Bucket
from logger import logger
from omegaconf import ListConfig
from polygon_reader_utils import read_region_polygons
from shapely import Polygon


def check_iou(poly1: Polygon, poly2: Polygon, threshold: float = 0.95) -> bool:
    """
    Computes IOU between 2 shapes
    """
    # Calculate the intersection and union areas
    intersection_area = poly1.intersection(poly2).area
    union_area = poly1.union(poly2).area

    # Calculate the overlap ratio
    overlap_ratio = intersection_area / union_area

    # Check if the overlap ratio is over threshold
    return overlap_ratio >= threshold


def convert_nested_listconfig(obj):
    """Recursively convert ListConfig to Python lists."""
    if isinstance(obj, ListConfig):
        return [convert_nested_listconfig(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: convert_nested_listconfig(v) for k, v in obj.items()}
    return obj


def check_polygon(
    cfg: SetupConfig, bucket: Bucket
) -> Tuple[List[List[float]], Optional[gpd.GeoDataFrame]]:
    """
    Checks is the polygon is correctly defined and if a list of sub-polygons can be returned for
    a wider region than an area.
    Args:
        cfg: config dictionary
        bucket: GCS bucket

    Returns:
        a tuple containing the custom or predefined list of coordinates, and a GeoDataFrame of sub-polygons, if exists
    """

    # Split area, by region level
    level_names = cfg.area.name.split("/")
    level = len(level_names)
    polygons_base_path = f"{cfg.polygons_database_path}/{cfg.area.name}/polygons"

    # Read polygons database
    main_polygon, sub_polygons_df = read_region_polygons(
        cfg.area.name, polygons_base_path, bucket, level
    )

    if main_polygon is not None:
        # Get custom defined polygon
        if cfg.area.polygon is not None:
            custom_polygon_reversed = Polygon(
                [(lon, lat) for lat, lon in cfg.area.polygon]
            )
            overlap = check_iou(main_polygon, custom_polygon_reversed, threshold=0.99)

            # If the database shape and custom defined ones overlap, return the database one
            if overlap:
                logger.info(
                    f"Polygon check -- DB polygon found but defined polygon will be used for {cfg.area.name}"
                )
                final_polygon = reverse_lat_lon(main_polygon)
            else:
                # Raise an error if no custom defined or pre-defined polygons were found
                message = f"Polygon check -- DB polygon exists, but the defined one does not overlap with it!"
                logger.error(message)
                raise ValueError(message)
        else:
            logger.info(f"Polygon check -- DB polygon is used for {cfg.area.name}")
            final_polygon = reverse_lat_lon(main_polygon)
    else:
        last_area = cfg.area.name.split("/")[-1]
        if "_custom" in last_area:
            # Get custom polygon path
            polygon_path = (
                f"{cfg.database_path}/{cfg.area.name}/{cfg.custom_polygon_filename}"
            )

            if cfg.area.polygon is not None:

                # Check if it exists:
                if bucket.blob(polygon_path).exists():
                    logger.info(
                        f"Polygon check -- Custom-defined polygon EXISTS and is used for {cfg.area.name}"
                    )
                else:
                    # If not, save polygon to database
                    geojson_data = {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "Polygon",
                                    "coordinates": convert_nested_listconfig(
                                        cfg.area.polygon
                                    ),
                                },
                                "properties": {},  # Add metadata if needed
                            }
                        ],
                    }

                    # Upload polygon
                    upload_json_to_gcs(bucket, polygon_path, geojson_data)

                    logger.info(
                        f"Polygon check -- Custom-defined polygon WAS UPLOADED and is used for {cfg.area.name}"
                    )

                # Create polygon
                final_polygon = Polygon(cfg.area.polygon)
            else:
                # Only if it exists, read it
                if bucket.blob(polygon_path).exists():
                    polygon_geojson = read_json_gcs(bucket, source_file=polygon_path)
                    polygon = polygon_geojson["features"][0]["geometry"]["coordinates"]
                    final_polygon = Polygon(polygon)
                else:
                    message = (
                        f"Polygon check -- You need to define a custom-polygon, "
                        f"because none in DB and you used '_custom' prefix!"
                    )
                    logger.error(message)
                    raise ValueError(message)
        else:
            # Raise an error if no custom defined or pre-defined polygons were found
            message = f"Polygon check -- No polygon in the DB!"
            logger.error(message)
            raise ValueError(message)

    polygon_coords = list(final_polygon.exterior.coords)

    return polygon_coords, sub_polygons_df

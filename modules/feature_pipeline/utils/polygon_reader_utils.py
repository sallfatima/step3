import os
from typing import Optional, Tuple

import geopandas as gpd
from google.cloud.storage import Bucket
from logger import logger
from polygon_readers import (
    DakarLengoPolygonReader,
    NigeriaPolygonReader,
    PolygonReaderFactory,
    SenegalPolygonReader,
)
from shapely import MultiPolygon, Polygon
from shapely.ops import unary_union


def convert_to_polygon(geometry):
    if isinstance(geometry, MultiPolygon):
        # Union the MultiPolygon into a single Polygon
        union = unary_union(geometry)

        # If still a MultiPolygon, transform to a convex hull
        if isinstance(union, MultiPolygon):
            return union.convex_hull

    return geometry


def read_region_polygons(
    region_path: str, base_path: str, bucket: Bucket, level: int
) -> Tuple[Optional[Polygon], Optional[gpd.GeoDataFrame]]:
    # Run the search process
    original_region_name = os.path.basename(region_path)
    while os.path.dirname(region_path) != "":
        region_name = os.path.basename(region_path)
        try:
            reader = polygon_factory.get_reader(region_name, base_path, bucket)
            reader.base_path = (
                f"{reader.base_path.split('/')[0]}/{region_path}/polygons"
            )
            logger.info(f"Polygons check -- Reading polygons from: {region_name}")
            polygons_union, polygons = reader.get_polygons(original_region_name, level)

            # Get rid of MultiPolygons and transform to Polygons
            polygons_union = (
                convert_to_polygon(polygons_union) if polygons_union else polygons_union
            )
            if polygons is not None:
                polygons["geometry"] = polygons["geometry"].apply(convert_to_polygon)

            return polygons_union, polygons

        except ValueError as e:
            logger.info(e)
            region_path = os.path.dirname(region_path)

    logger.warning(f"No polygons found for region: {original_region_name}")

    return None, None


# Setup
polygon_factory = PolygonReaderFactory()

# Register region-specific readers
polygon_factory.register_reader("nigeria", NigeriaPolygonReader)
polygon_factory.register_reader("senegal", SenegalPolygonReader)
polygon_factory.register_reader("dakar_lengo", DakarLengoPolygonReader)

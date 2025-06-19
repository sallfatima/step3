from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple, Type

import geopandas as gpd
from cloud_utils import read_shapefiles
from google.cloud.storage import Bucket
from logger import logger
from shapely import MultiPolygon, Polygon
from shapely.ops import unary_union
from unidecode import unidecode


class PolygonReader(ABC):
    def __init__(self, base_path: str, bucket: Bucket):
        self.base_path = base_path
        self.bucket = bucket

        # Initialize polygons dataframe
        self.polygons_df: Optional[gpd.GeoDataFrame] = None

    def set_polygons_df(self, polygons_name: str) -> None:
        """
        Reads and sets polygons dataframe
        Args:
            polygons_name: string representing the path/common_name of shapefiles
        """

        # Set base path and name for the polygons
        self.base_path = f"{self.base_path}/{polygons_name}"

        # Read shapefiles
        self.polygons_df = read_shapefiles(self.base_path, self.bucket)

        # Standardize polygons
        self.standardize_df()

    def filter(self, original_region_name: str, column: str) -> None:
        """
        Helper method to filter and standardize the dataframe based on a column.
        Args:
            original_region_name: name of the region
            column: name of the column to check for the region name
        """

        logger.info(
            f"Polygon reader -- Filtering for {original_region_name} in column {column}"
        )
        self.polygons_df = self.polygons_df[
            self.polygons_df[column] == original_region_name
        ]
        if self.polygons_df.empty:
            raise ValueError(
                f"Polygon check -- No polygons for region {original_region_name} in DB. "
            )

    @abstractmethod
    def get_polygons(
        self, region_name: str, level: int
    ) -> Tuple[Polygon, Optional[gpd.GeoDataFrame]]:
        """
        Retrieves polygons for a region
        Args:
            region_name: name of the region
            level: the level in the database

        Returns:
            a tuple representing the polygon of the region and a GeoDataFrame with sub-polygons
        """
        raise NotImplementedError("This method should be implemented by subclasses")

    @abstractmethod
    def standardize_df(self) -> None:
        """Function thar standardizes the GeoDataFrame for a region.
        This usually involves lowercasing, replacing " " with "_", and "/" with "-"."""

        raise NotImplementedError("This method should be implemented by subclasses")

    @staticmethod
    def convert_to_polygon(geometry):
        if isinstance(geometry, MultiPolygon):
            # Union the MultiPolygon into a single Polygon
            return unary_union(geometry)
        return geometry  # Keep Polygon geometries unchanged


class NigeriaPolygonReader(PolygonReader):
    """Class for reading polygons for Nigeria"""

    def get_polygons(
        self, original_region_name, level
    ) -> Tuple[Polygon, Optional[gpd.GeoDataFrame]]:

        # Sets the polygons dataframe
        self.set_polygons_df("nigeria")

        if level == 2:
            pass
        elif level == 3:
            region_df = self.polygons_df[
                self.polygons_df["statename"] == original_region_name
            ]
            if len(region_df) == 0:
                raise ValueError(
                    f"Polygon check -- No pre-defined polygons for user-defined area"
                    f" {original_region_name}"
                )
            self.polygons_df = region_df

            # self.filter(original_region_name, 'statename')

        else:
            region_df = self.polygons_df[
                self.polygons_df["lganame"] == original_region_name
            ]
            if len(region_df) == 0:
                region_df = self.polygons_df[
                    self.polygons_df["wardname"] == original_region_name
                ]
                if len(region_df) == 0:
                    raise ValueError(
                        f"Polygon check -- No pre-defined polygons for user-defined area"
                        f" {original_region_name}"
                    )
            self.polygons_df = region_df

        # Get the union of sub-polygons
        polygons_union = unary_union(self.polygons_df.geometry)

        # Rename columns
        self.polygons_df = self.polygons_df.rename(columns={"wardname": "name"})
        self.polygons_df = self.polygons_df[["name", "geometry"]]

        # If the dataframe contains one row, it means no real sub-polygons present
        if len(self.polygons_df) == 1:
            return polygons_union, None

        return polygons_union, self.polygons_df

    def standardize_df(self):
        columns = ["wardname", "lganame", "statename"]
        for column in columns:
            self.polygons_df.loc[:, column] = (
                self.polygons_df[column]
                .astype(str)
                .str.lower()
                .apply(
                    lambda x: x.replace(" / ", "-").replace(" ", "_").replace("/", "-")
                )
                .apply(unidecode)
            )


class DakarLengoPolygonReader(PolygonReader):
    """Class for reading polygons for Nigeria"""

    def get_polygons(
        self, original_region_name, level
    ) -> Tuple[Polygon, Optional[gpd.GeoDataFrame]]:

        # Sets the polygons dataframe
        self.set_polygons_df("dakar_quartiers-polygon")

        # Increase level
        if level == 3:
            pass
        else:
            region_df = self.polygons_df[
                self.polygons_df["zone"] == original_region_name
            ]
            if len(region_df) == 0:
                region_df = self.polygons_df[
                    self.polygons_df["quartiers"] == original_region_name
                ]
                if len(region_df) == 0:
                    raise ValueError(
                        f"Polygon check -- No pre-defined polygons for user-defined area"
                        f" {original_region_name}"
                    )
            self.polygons_df = region_df

        # Get the union of sub-polygons
        polygons_union = unary_union(self.polygons_df.geometry)

        # Rename columns
        self.polygons_df = self.polygons_df.rename(columns={"quartiers": "name"})
        self.polygons_df = self.polygons_df[["name", "geometry"]]

        # If the dataframe contains one row, it means no real sub-polygons present
        if len(self.polygons_df) == 1:
            return polygons_union, None

        return polygons_union, self.polygons_df

    def standardize_df(self):
        columns = ["code", "zone", "quartiers"]
        for column in columns:
            self.polygons_df.loc[:, column] = (
                self.polygons_df[column]
                .astype(str)
                .str.lower()
                .apply(lambda x: x.replace(" - ", "-").replace(" ", "_"))
                .apply(unidecode)
                .apply(lambda x: f"{x}_lengo")
            )


class SenegalPolygonReader(PolygonReader):
    """Class for reading polygons for Nigeria"""

    def get_polygons(
        self, original_region_name, level
    ) -> Tuple[Polygon, Optional[gpd.GeoDataFrame]]:

        # Sets the polygons dataframe
        self.set_polygons_df("senegal")

        if level == 2:
            pass
        elif level == 3:
            region_df = self.polygons_df[
                self.polygons_df["region"] == original_region_name
            ]
            if len(region_df) == 0:
                raise ValueError(
                    f"Polygon check -- No pre-defined polygons for user-defined area"
                    f" {original_region_name}"
                )
            self.polygons_df = region_df

            # self.filter(original_region_name, 'region')

        else:
            region_df = self.polygons_df[
                self.polygons_df["department"] == original_region_name
            ]
            if len(region_df) == 0:
                region_df = self.polygons_df[
                    self.polygons_df["arrondissment"] == original_region_name
                ]
                if len(region_df) == 0:
                    raise ValueError(
                        f"Polygon check -- No pre-defined polygons for user-defined area"
                        f" {original_region_name}"
                    )
            self.polygons_df = region_df

        # Get the union of sub-polygons
        polygons_union = unary_union(self.polygons_df.geometry)

        # Rename columns
        self.polygons_df = self.polygons_df.rename(columns={"arrondissment": "name"})
        self.polygons_df = self.polygons_df[["name", "geometry"]]

        # If the dataframe contains one row, it means no real sub-polygons present
        if len(self.polygons_df) == 1:
            return polygons_union, None

        return polygons_union, self.polygons_df

    def standardize_df(self):
        columns_renamed = ["arrondissment", "department", "region"]
        columns = ["ADM3_FR", "ADM2_FR", "ADM1_FR"]
        self.polygons_df = self.polygons_df.rename(
            columns=dict(zip(columns, columns_renamed))
        )

        for column in columns_renamed:
            self.polygons_df.loc[:, column] = (
                self.polygons_df[column]
                .astype(str)
                .str.lower()
                .apply(lambda x: x.replace(" ", "_").replace("n/a", "unknown"))
                .apply(unidecode)
            )


class PolygonReaderFactory:
    def __init__(self):
        self.readers: Dict[str, Type[PolygonReader]] = {}

    def register_reader(self, region: str, reader: Type[PolygonReader]) -> None:
        self.readers[region] = reader

    def get_reader(self, region: str, base_path: str, bucket: Bucket) -> PolygonReader:
        reader_class = self.readers.get(region)
        if not reader_class:
            raise ValueError(
                f"Polygons check -- No reader registered for region: {region}"
            )
        return reader_class(base_path, bucket)

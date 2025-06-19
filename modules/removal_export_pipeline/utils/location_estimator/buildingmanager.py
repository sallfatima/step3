import geopandas as gpd
import pandas as pd
import psutil
from location_estimator.latlng import LatLng
from logger import logger
from shapely import wkt
from shapely.geometry import MultiPolygon, Polygon, box
from shapely.wkt import loads


class Building:
    def __init__(self, ref, nodes):
        self.ref = ref
        self.nodes = nodes
        self.labels = []


class BuildingManager:
    def __init__(self, csv_filename: str, bounding_box: tuple = None):
        self.buildings = []
        self.nodes_dict = {}
        self.bounding_box = bounding_box  # Store the bounding box if provided

        # Load and filter OpenBuildings data
        self.load_and_filter_buildings(csv_filename)

        # Create a spatial grid for efficient querying
        self.create_grid()

    def load_and_filter_buildings(self, csv_filename: str) -> None:
        """Loads and filters building data from the OpenBuildings CSV file using the bounding box."""

        logger.info(
            f"Free memory before loading buildings = {psutil.virtual_memory().available / (1024 ** 2):.2f} MB"
        )
        logger.info(f"Loading buildings data from {csv_filename}")

        dtype_dict = {
            "latitude": "float64",
            "longitude": "float64",
            "area_in_meters": "float64",
            "confidence": "float64",
            "geometry": "object",
            "full_plus_code": "object",
        }
        if csv_filename.endswith(".gz"):
            # Read a compressed CSV file
            df = pd.read_csv(
                csv_filename, compression="gzip", dtype=dtype_dict, low_memory=False
            )
        else:
            # Read a regular CSV file
            df = pd.read_csv(csv_filename, dtype=dtype_dict, low_memory=False)

        logger.info(
            f"Free memory after loading buildings = {psutil.virtual_memory().available / (1024 ** 2):.2f} MB"
        )
        logger.info("Transform to GeoDataFrame...")

        # Check for the required 'geometry' column
        df["geometry_polygon"] = df["geometry"].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry="geometry_polygon").set_crs("EPSG:4326")

        # Define the bounding box using shapely bbox
        (
            top_left_lat,
            top_left_lon,
            bottom_right_lat,
            bottom_right_lon,
        ) = self.bounding_box
        bounding_box_polygon = box(
            top_left_lon, bottom_right_lat, bottom_right_lon, top_left_lat
        )  # Create bounding box

        logger.info(
            f"Free memory before transforming buildings = {psutil.virtual_memory().available / (1024 ** 2):.2f} MB"
        )
        logger.info("Filtering buildings dataframe...")

        # Filter gdf
        gdf_sindex = gdf.sindex
        possible_matches_index = list(
            gdf_sindex.intersection(bounding_box_polygon.bounds)
        )
        possible_matches = gdf.iloc[possible_matches_index]
        filtered_df = possible_matches[
            possible_matches.intersects(bounding_box_polygon)
        ]

        logger.info(f"Filtered {len(filtered_df)} buildings within the bounding box.")

        # Convert filtered rows to Building objects
        building_count = 0
        for idx, row in filtered_df.iterrows():
            try:
                # Parse the geometry for each building
                building_geom = self.parse_geometry(row["geometry"])
                if not building_geom:
                    continue

                nodes = []
                if isinstance(building_geom, Polygon):
                    # For simple polygons, use all exterior coordinates
                    nodes = [
                        LatLng(lat, lon) for lon, lat in building_geom.exterior.coords
                    ]
                elif isinstance(building_geom, MultiPolygon):
                    # For multipolygons, combine coordinates from all constituent polygons
                    nodes = [
                        LatLng(lat, lon)
                        for poly in building_geom.geoms
                        for lon, lat in poly.exterior.coords
                    ]

                if nodes:  # Only create building if we have valid nodes
                    self.buildings.append(Building(idx, nodes))
                    building_count += 1

            except Exception as e:
                logger.warning(f"Failed to process building {idx}: {str(e)}")
                continue

        # # Convert filtered rows to Building objects
        # for idx, row in filtered_df.iterrows():
        #     # Parse the geometry for each building
        #     building_polygon = self.parse_geometry(row["geometry"])
        #     if building_polygon:
        #         # Convert the polygon's coordinates into nodes and create a Building object
        #         nodes = [
        #             LatLng(lat, lon) for lon, lat in building_polygon.exterior.coords
        #         ]
        #         self.buildings.append(Building(idx, nodes))

    @staticmethod
    def is_building_in_bbox(geometry_str: str, bounding_box_poly: Polygon) -> bool:
        """Checks if a building polygon intersects with the given bounding box."""

        # Create a shapely Polygon from the WKT geometry string
        building_polygon = BuildingManager.parse_geometry(geometry_str)
        return building_polygon.intersects(bounding_box_poly)

    @staticmethod
    def parse_geometry(geometry_str: str):
        """Parses a WKT geometry string and returns a shapely geometry object.
        Returns None for unsupported or invalid geometries."""
        try:
            geom = loads(geometry_str)

            if geom.is_empty:
                return None

            if geom.geom_type in ("Polygon", "MultiPolygon"):
                return geom

            logger.warning(f"Unsupported geometry type: {geom.geom_type}")
            return None
        except Exception as e:
            logger.warning(f"Error parsing geometry: {str(e)}")
            return None

    def filter_buildings_by_bounding_box(self) -> None:
        """Filters buildings to keep only those within the specified bounding box."""
        (
            top_left_lat,
            top_left_lon,
            bottom_right_lat,
            bottom_right_lon,
        ) = self.bounding_box

        # Keep only buildings with nodes inside the bounding box
        filtered_buildings = []
        for building in self.buildings:
            # Check if any node of the building is inside the bounding box
            if any(
                top_left_lat >= node.lat >= bottom_right_lat
                and top_left_lon <= node.lng <= bottom_right_lon
                for node in building.nodes
            ):
                filtered_buildings.append(building)

        # Update the buildings list
        self.buildings = filtered_buildings

    def create_grid(self, interval=50):
        self.building_grid = []
        self.interval = interval

        max_lat = -999999
        max_lng = -999999
        min_lat = 9999999
        min_lng = 9999999

        # Calculate bounding box for all buildings
        for b in self.buildings:
            for nd in b.nodes:
                max_lat = max(max_lat, nd.lat)
                max_lng = max(max_lng, nd.lng)
                min_lat = min(min_lat, nd.lat)
                min_lng = min(min_lng, nd.lng)

        # Calculate grid dimensions
        width = abs(LatLng(min_lat, min_lng).get_xy(LatLng(max_lat, max_lng)).x)
        height = abs(LatLng(min_lat, min_lng).get_xy(LatLng(max_lat, max_lng)).y)
        self.min_lat = min_lat
        self.min_lng = min_lng

        self.nx = int(width / interval) + 3
        self.ny = int(height / interval) + 3

        # Create an empty grid
        for i in range(self.nx):
            tmp = []
            for j in range(self.ny):
                tmp.append([])
            self.building_grid.append(tmp)

        # Populate the grid with buildings
        for b in self.buildings:
            for nd in b.nodes:
                x = abs(LatLng(min_lat, min_lng).get_xy(nd).x)
                y = abs(LatLng(min_lat, min_lng).get_xy(nd).y)
                self.building_grid[int(x / interval) + 1][int(y / interval) + 1].append(
                    b
                )

    def find_buildings(self, point):
        x = abs(LatLng(self.min_lat, self.min_lng).get_xy(point).x)
        y = abs(LatLng(self.min_lat, self.min_lng).get_xy(point).y)
        nearest = []
        for i in range(-2, 4):
            for j in range(-2, 4):
                if 0 <= int(x / self.interval) + i < self.nx:
                    if 0 <= int(y / self.interval) + j < self.ny:
                        for b in self.building_grid[int(x / self.interval) + i][
                            int(y / self.interval) + j
                        ]:
                            if b not in nearest:
                                nearest.append(b)
        return nearest

    def find_nearest_building(self, point):
        blist = self.find_buildings(point)
        min_dis = 9999999
        n_building = None
        for building in blist:
            for n in building.nodes:
                dis = point.get_distance(n)
                if dis < min_dis:
                    min_dis = dis
                    n_building = building
        return n_building

import gc
import os
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import geopandas as gpd
import networkx as nx
import networkx.readwrite.json_graph as json_graph
import pandas as pd
import tqdm
from cloud_utils import read_json_gcs
from config_model import SetupConfig
from google.cloud.storage import Bucket
from joblib import Parallel, delayed
from logger import logger
from shapely import Polygon
from shapely.strtree import STRtree
from viz_utils import plot_card


def get_dates(g_sv: nx.Graph, start_year: int = 2013) -> pd.DataFrame:
    """
    Optimized version of get_dates for large graphs using vectorized operations.

    Args:
        g_sv: NetworkX graph of SV
        start_year: Starting year for dates distribution

    Returns:
        DataFrame with dates percentages
    """
    # Extract all 'date' attributes where 'date' exists and is a string
    dates = [
        data["date"]
        for _, data in g_sv.nodes(data=True)
        if "date" in data and isinstance(data["date"], str)
    ]

    # Create a DataFrame
    df_dates = pd.DataFrame(dates, columns=["date"])

    # Convert 'date' to datetime and extract 'year'
    df_dates["year"] = pd.to_datetime(
        df_dates["date"], format="%Y-%m", errors="coerce"
    ).dt.year

    # Drop rows with invalid dates
    df_dates = df_dates.dropna(subset=["year"])

    # Filter by start_year
    df_filtered = df_dates[df_dates["year"] >= start_year]

    # Count occurrences per year
    counts = df_filtered["year"].value_counts().sort_index()

    # Compute percentages
    total_panoramas = counts.sum()
    percentages = (counts / total_panoramas * 100).round(2).astype(str)

    # Create DataFrame for percentages
    df_percentage = percentages.reset_index()
    df_percentage.columns = ["year", "percentage"]

    # Ensure all years are present
    current_year = datetime.now().year
    all_years = pd.DataFrame({"year": range(start_year, current_year + 1)})

    # Merge to include missing years with 0.0%
    df_final = all_years.merge(df_percentage, on="year", how="left").fillna(0.0)
    df_final["year"] = df_final["year"].astype(str)
    df_final["percentage"] = df_final["percentage"].astype(str)

    return df_final


def create_str_tree(
    g: nx.Graph,
) -> Tuple[Optional[STRtree], Optional[gpd.GeoDataFrame]]:
    if not g:
        return None, None

    # Create GeoDataFrame directly from the graph
    nodes = list(g.nodes(data=True))
    node_ids = [node[0] for node in nodes]
    latitudes = [node[1]["lat"] for node in nodes]
    longitudes = [node[1]["lon"] for node in nodes]

    # Create a GeoDataFrame with Point geometries
    gdf = gpd.GeoDataFrame(
        {"node_id": node_ids, "lat": latitudes, "lon": longitudes},
        geometry=gpd.points_from_xy(longitudes, latitudes),
        crs="EPSG:4326",
    )

    # Create STRtree for spatial indexing
    str_tree = STRtree(gdf.geometry)

    return str_tree, gdf


def keep_nodes_str(
    g: nx.Graph, polygon: Polygon, gdf: gpd.GeoDataFrame, str_tree: STRtree
) -> nx.Graph:
    # # Query the bounding box to get candidate geometries
    bounding_box = polygon.bounds  # (minx, miny, maxx, maxy)
    candidates_bbox = str_tree.query(Polygon.from_bounds(*bounding_box))

    # Filter candidates with precise polygon.contains checks
    nodes_within_polygon = [
        gdf.iloc[i]["node_id"]
        for i in candidates_bbox
        if polygon.contains(gdf.iloc[i].geometry)
    ]

    # Filter the original graph using subgraph view
    return g.subgraph(nodes_within_polygon)


def compute_total_length(g: nx.Graph) -> float:
    """
    Efficiently computes the total road length from edge distances in the graph.

    Args:
        g: NetworkX graph with 'distance' attribute on edges.

    Returns:
        Total road length in kilometers.
    """
    # Extract all 'distance' attributes at once
    total_length_meters = sum([data["distance"] for _, _, data in g.edges(data=True)])

    return total_length_meters / 1000.0  # Convert to kilometers


def process_row(
    index: Any,
    sv_graph,
    osm_graph,
) -> Dict[str, int | float]:
    """Optimized process_row function for coverage distribution"""

    sv_graph = pickle.loads(sv_graph)
    osm_graph = pickle.loads(osm_graph)

    result = {"ID": index}

    # Compute road length SV
    total_sv_length = (
        compute_total_length(sv_graph) if sv_graph.number_of_nodes() > 0 else 0.0
    )

    # Compute road length OSM
    total_osm_length = (
        compute_total_length(osm_graph) if osm_graph.number_of_nodes() > 0 else 0.00001
    )

    result["total_sv"] = total_sv_length
    result["total_osm"] = total_osm_length
    result["coverage"] = (
        (total_sv_length / total_osm_length * 100) if total_osm_length > 0 else 0.0
    )
    logger.info(f"Card -- processed row index {index}")

    # del osm_graph
    # del sv_graph
    # gc.collect()

    return result


def get_stats(
    cfg: SetupConfig,
    gdf: gpd.GeoDataFrame,
    sv_graph: nx.Graph,
    osm_graph: nx.Graph,
    bucket: Bucket,
    start_year: int = 2010,
    main_card: bool = False,
) -> gpd.GeoDataFrame:
    """
    Optimized get_stats function using vectorization and parallel processing.

    Args:
        cfg: Configuration object
        gdf: GeoPandas DataFrame with the area of interest
        sv_graph: SV graph (NetworkX)
        osm_graph: OSM graph (NetworkX)
        bucket: Storage bucket object
        start_year: Starting year for statistics
        main_card: Boolean indicating if processing the main area

    Returns:
        GeoDataFrame with computed statistics
    """

    # Get card config
    card_config = cfg.features.card

    # Initialize DataFrame for dates percentages
    dates_df = pd.DataFrame(columns=["ID", "year", "percentage"])

    # Create STRtree and GeoDataFrame from OSM and SV graphs
    str_tree_osm, osm_gdf = create_str_tree(osm_graph)
    str_tree_sv, sv_gdf = create_str_tree(sv_graph)

    logger.info("Splitting graphs")

    # Split OSM and SV graphs into subgraphs
    filtered_svs = {}
    filtered_osms = {}
    sv_dates_dfs = {}

    def filter_graphs(index, geometry):
        """Filters the SV and OSM graphs based on the provided geometry."""
        sv_filtered = keep_nodes_str(sv_graph, geometry, sv_gdf, str_tree_sv)
        osm_filtered = keep_nodes_str(osm_graph, geometry, osm_gdf, str_tree_osm)

        sv_df = get_dates(sv_filtered, start_year)
        sv_df["ID"] = index

        return index, sv_filtered, osm_filtered, sv_df

    if main_card:
        filtered_svs[0] = sv_graph
        filtered_osms[0] = osm_graph
        sv_dates_dfs[0] = get_dates(sv_graph, start_year)
        sv_dates_dfs[0]["ID"] = 0
    else:
        # Parallelize the filtering process
        with ThreadPoolExecutor(max_workers=card_config.max_workers) as executor:
            futures = {
                executor.submit(
                    filter_graphs,
                    index,
                    row.geometry,
                ): index
                for index, row in gdf.iterrows()
            }
            for future in tqdm.tqdm(as_completed(futures), total=len(futures)):
                index, filtered_sv, filtered_osm, sv_dates_df = future.result()
                filtered_svs[index] = filtered_sv
                filtered_osms[index] = filtered_osm
                sv_dates_dfs[index] = sv_dates_df

    # free memory
    del osm_gdf
    del sv_gdf
    del str_tree_sv
    del str_tree_osm
    del sv_graph
    del osm_graph
    gc.collect()

    logger.info("Start of processing")

    # Process rows in parallel using joblib's Parallel
    results_list = Parallel(n_jobs=card_config.max_workers)(
        delayed(process_row)(
            index,
            pickle.dumps(filtered_svs[index]),
            pickle.dumps(filtered_osms[index]),
        )
        for index, row in tqdm.tqdm(gdf.iterrows(), total=len(gdf))
    )

    # After parallel processing, update the gdf DataFrame and dates_df
    for i, result in enumerate(results_list):
        # Update gdf with the results
        for key, value in result.items():
            gdf.at[result["ID"], key] = value
        # Concatenate the row-specific dates_df with the main dates_df
        dates_df = pd.concat([dates_df, sv_dates_dfs[result["ID"]]], ignore_index=True)

    # Convert column types and sort years
    dates_df["year"] = dates_df["year"].astype(int)
    dates_df["percentage"] = dates_df["percentage"].astype(float)
    dates_df = dates_df.sort_values(by="year")

    # Group by 'ID' and compute cumulative percentages
    cumulative = (
        dates_df[dates_df["year"] >= start_year]
        .groupby("ID")
        .apply(
            lambda x: x.sort_values("year", ascending=True).assign(
                cumulative_percentage=x["percentage"][::-1].cumsum()[::-1]
            )
        )
        .reset_index(drop=True)
    )

    # Prepare 'years' column
    cumulative["years"] = cumulative["year"].astype(str) + f" - {datetime.now().year}"

    # Select necessary columns
    cumulative = cumulative[["ID", "years", "cumulative_percentage"]]

    # Merge coverage dataframe with the cumulative percentages one
    df = gdf.merge(cumulative, on="ID", how="left")
    gdf = gpd.GeoDataFrame(df, geometry="geometry").set_crs("epsg:4326")

    # Convert column types for plotting
    gdf["years"] = gdf["years"].astype(str)
    gdf["cumulative_percentage"] = gdf["cumulative_percentage"].astype(float).round(2)
    gdf["coverage"] = gdf["coverage"].astype(float).round(2)
    gdf["total_sv"] = gdf["total_sv"].astype(float).round(2)
    gdf["total_osm"] = gdf["total_osm"].astype(float).round(2)

    return gdf


def build_card(
    cfg: SetupConfig, bucket: Bucket, sub_polygons_df: gpd.GeoDataFrame
) -> None:
    """
    Builds a knowledge card with coverage and dates percentages and saves it to cloud
    Args:
        cfg: configuration dictionary
        bucket: GCS bucket
        sub_polygons_df: GeoDataFrame with sub-polygons
    """

    # Get graph of SV data
    sv_graph_path = f"{cfg.area.data_path}/{cfg.sv_name}_merged.json"
    sv_merged_exists = bucket.blob(sv_graph_path).exists()
    assert sv_merged_exists, logger.error(
        f"Card -- SV map not computed. Please run build process before!"
    )

    # Get graph of SV data
    osm_graph_path = f"{cfg.area.data_path}/{cfg.osm_name}_merged.json"
    osm_merged_exists = bucket.blob(osm_graph_path).exists()
    assert osm_merged_exists, logger.error(
        f"Card -- OSM map not computed. Please run build process before!"
    )

    # Get graph SV data
    json_sv = read_json_gcs(bucket, sv_graph_path)
    g_sv = json_graph.adjacency_graph(json_sv)

    # Get graph of OSM data
    json_osm = read_json_gcs(bucket, osm_graph_path)
    g_osm = json_graph.adjacency_graph(json_osm)

    # Get area name
    area_name = os.path.basename(cfg.area.name)

    # Define columns
    columns = ["ID", "name", "total_sv", "total_osm", "coverage"]

    # Check existence and compute main card
    output_main_card_path = f"{cfg.area.data_path}/card_main.html"
    main_card_exists = bucket.blob(output_main_card_path).exists()

    if not main_card_exists or cfg.features.card.force_recompute:

        # Reverse lat/lon coordinates (NEEDED for correct Mapbox plotting)
        area_geom = Polygon([(lon, lat) for lat, lon in cfg.area.polygon])

        # Initialize necessary GeoDataFrame
        gdf = gpd.GeoDataFrame(geometry=[area_geom], columns=columns).set_crs(
            "epsg:4326"
        )
        gdf["name"] = area_name

        # Compute statistics
        gdf = get_stats(cfg, gdf, g_sv, g_osm, bucket, main_card=True)

        # Plots dates and coverage map
        plot_card(gdf, output_main_card_path, bucket)

        logger.info("Card - retrieved for main card")
    else:
        logger.info("Card - already computed for main card")

    # Compute secondary map based on sub_polygons division
    if sub_polygons_df is not None and len(sub_polygons_df) > 0:
        output_secondary_card_path = f"{cfg.area.data_path}/card_secondary.html"
        secondary_card_exists = bucket.blob(output_secondary_card_path).exists()
        if not secondary_card_exists or cfg.features.card.force_recompute:
            # Initialize necessary GeoDataFrame
            columns.remove("name")
            sub_polygons_df[columns] = None

            # Compute statistics
            gdf = get_stats(cfg, sub_polygons_df, g_sv, g_osm, bucket, main_card=False)

            # Plots dates and coverage map
            plot_card(gdf, output_secondary_card_path, bucket)

            logger.info("Card - retrieved for secondary card")
        else:
            logger.info("Card - already computed for secondary card")

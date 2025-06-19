import ast
import os
from typing import List, Tuple

import networkx as nx
from logger import logger
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
import datetime

def driver_setup() -> WebDriver:
    """Sets up a headless Chrome webdriver"""

    # Set options and return driver
    op = webdriver.ChromeOptions()
    op.add_argument("--no-sandbox")
    op.add_argument("--headless")
    op.add_argument("--disable-gpu")
    op.add_argument("--disable-dev-shm-usage")
    op.add_argument("--dns-prefetch-disable")

    return webdriver.Chrome(options=op)


def nodes_to_string(g: nx.Graph) -> Tuple[str, str]:
    """Create string representations of (latitudes, longitudes) coordinates of points in graph"""
    string_lats = ""
    string_lons = ""
    for _, node in g.nodes(data=True):
        string_lats += f"{node['lat']},"
        string_lons += f"{node['lon']},"

    return string_lats[:-1], string_lons[:-1]


def replace_api_key(api_key: str) -> str:
    """
    Replaces an API KEY into a predefined HTML file
    Args:
        api_key: api key (Google)

    Returns:
        Path to the temporary HTML file with the replaced API key
    """
    # Read the content of the HTML file
    with open("./utils/finder_utils/index.html", "r") as file:
        html_content = file.read()

    # Replace the placeholder with the actual API key
    html_content = html_content.replace("YOUR_API_KEY", api_key)

    # Save the modified content to a temporary file
    output_html = "temp_index.html"
    with open(output_html, "w") as temp_file:
        temp_file.write(html_content)

    return os.path.abspath(output_html)


def find(
    g: nx.Graph,
    html_file_path: str,
    driver: WebDriver,
    radius: int,
    cfg
) -> Tuple[List[List[str]], Tuple[float, ...], Tuple[float, ...]]:
    """
    Finds available SV locations
    Args:
        g: graph of OS points
        html_file_path: path to HTML file to be loaded in the headless browser
        driver: chrome webdriver
        radius: radius in meters, set around each OSM point, and used in panorama search

    Returns:
        (available_locations_dates, lats, lons) with:

        available_locations_dates -> [['(sv_lat1, sv_lon1)', 'sv_date1'], ['(sv_lat2, sv_lon2)', 'sv_date2'], ....]
        lats -> (lat1, lat2, .....)
        lons -> (lon1, lon2, .....)
    """

    # Transform nodes coords to string
    string_lats, string_lons = nodes_to_string(g)

    # Open the HTML file in the browser
    try:
        driver.get(f"file://{html_file_path}")
    except Exception as e:
        logger.error(f"Inside run error is: {e}")

    # Set timeout for the script
    driver.set_script_timeout(1200)

    # Load the JS code in memory
    with open("utils/finder_utils/find.js", "r") as f:
        _ = f.read()

    # Execute the string command ( taken from the JS file loaded into memory)
    available_locations_dates = driver.execute_script(
        "return run(arguments[0], arguments[1], arguments[2]);",
        string_lats,
        string_lons,
        radius,
    )
    # -----------------------------------------------
    # FILTER STEP: Keep only dates between config start and end
    # -----------------------------------------------

    # # Get the date range from the configuration
    # build_cfg = cfg.features.build
    # start_str = build_cfg.date_range.start  # 'YYYY-MM-DD'
    # end_str   = build_cfg.date_range.end    # 'YYYY-MM-DD'
    # # convert to datetime objects
    # start_dt = datetime.datetime.strptime(start_str, "%Y-%m-%d")
    # end_dt   = datetime.datetime.strptime(end_str,   "%Y-%m-%d")

    # filtered_locations = []
    # for coord_pair, date_str in available_locations_dates:
    #     # date_str is in 'YYYYMMDD' format; convert to a datetime object
    #     try:
    #         img_date = datetime.datetime.strptime(date_str, "%Y%m%d")
    #     except ValueError:
    #         # If the date string is malformed for any reason, skip it
    #         continue

    #     # If the image date is within the inclusive range start_dt to end_dt, keep it
    #     if start_dt <= img_date <= end_dt:
    #         filtered_locations.append([coord_pair, date_str])

    # # Replace the original list with our filtered list
    # available_locations_dates = filtered_locations
    # Convert to lists of numbers
    lats = ast.literal_eval(string_lats)
    lons = ast.literal_eval(string_lons)

    if not isinstance(lats, tuple):
        lats = tuple([lats])

    if not isinstance(lons, tuple):
        lons = tuple([lons])

    return available_locations_dates, lats, lons

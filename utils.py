import os
import json
import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from sql_queries import queries


username = 'sa'
password = 'D0nkeyK0ng!'
database = 'divvy'
host = 'localhost'
port = 1433


def construct_element_dict(
        elements: list
) -> dict:
    all_tags = {}

    for element in elements:
        if element.text not in all_tags.keys():
            all_tags[element.text] = element

    return all_tags


def configure_sqlalchemy_conn(
        username: str,
        password: str,
        database: str,
        host: str,
        port: int = 1433
):
    engine = create_engine(
        f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC Driver 17 for SQL Server")
    return engine


def create_dated_directory(parent_directory) -> str:
    curr_date = str(datetime.datetime.now())
    print(curr_date[:10])

    parent_dir = parent_directory
    os.chdir(parent_dir)
    download_directory_path = parent_dir + curr_date[:10] + "\\"

    if curr_date[:10] not in os.listdir(parent_dir):
        print("No destination directory for current date. Creating new directory...")
        os.mkdir(curr_date[:11])
        print(os.listdir(parent_dir))
    else:
        print("Directory path exists: ", parent_dir + curr_date[:10] + "\\")

    return download_directory_path


def read_configuration_json(filepath):
    with open(filepath, 'r') as file:
        config = json.load(file)
    return config


def setup_driver(parent_download_dir: str = None) -> webdriver.Firefox:
    download_directory = create_dated_directory(parent_download_dir)

    options = Options()
    options.set_preference("browser.download.folderList", 2)
    options.set_preference(
        "browser.download.manager.showWhenStarting", True)
    options.set_preference(
        "browser.download.dir", download_directory)
    options.set_preference(
        "browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")
    options.add_argument('--headless')
    driver = webdriver.Firefox(
        options=options)

    return driver


def get_ingested_filenames(engine):
    # TODO: This might not actually need to be a separate function at all
    # because I'm only expecting to call this in one place and won't need
    # repeated usages.
    with engine.connect() as conn:
        filenames = conn.execute(queries['select_ingested_tables'])
        return filenames


def update_ingested_files(engine, filename):
    with engine.connect() as conn:
        # This line will write the table name to divvy.dbo.ingested_files
        conn.execute(queries['insert_table_retrieved'].bindparams(
            source_file=filename))
        conn.commit()


def create_station_relations_graph():
    """
    This function should take a parsed rides table, join with the stations
    table to get all applicable data.

    The function should then:
        1) Construct a graph which details all of the stations which are
            connected to other stations via trips.
        2) For each distinct pair of start_station_id and end_station_id,
            we will need to supply starting coordinates and ending coordinates.
        3) These coordinates will then be fed into the Directions API to
            generate a trip route.
        4) Once we've created the route, this can  estimate trip duration,
            distance traveled, and to plot the route taken via Mapbox.
        5) This route should be fed into our simulation to allow the display
            of the trips throughout the city.
        6) To persist this data, we should reference the station_id of both the 
            starting and ending station as a composite primary key.
    """
    ...

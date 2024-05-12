import os
import time
import typing
import zipfile
import polars as pl
import sqlalchemy as sa
from utils import (
    configure_sqlalchemy_conn,
    construct_element_dict,
    create_dated_directory,
    setup_driver
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sql_queries import (
    # table_retrieved,
    ingested_tables,
    divvy_trips_dbo,
    divvy_trips_raw
)


"""
    EXTRACTION

    This module focuses entirely on the process of extracting data from 
    where it is hosted online into a format which can be loaded into a 
    database. The URL https://divvy-tripdata.s3.amazonaws.com/index.html 
    contains links to a number of .CSV files which contain ridership data 
    for the Divvy bike system in Chicago.

    This utility uses Selenium to create a headless browser, navigate to 
    the page, and create a list of all of the links to download .CSV files 
    from the public S3 bucket. MVP is to create a bare-bones implementation 
    which will download the file and host it in a target directory. Once the 
    CSV file has been downloaded, it will need to be unzipped. This will end 
    the scope of the "extraction" module. Ideally scraping and extraction 
    could be separated into their own modules for ease of testing and
    maintenance.

    A stretch goal for this module is to use an environment variable to specify
    the working directory for the downloaded file. This would be useful for 
    later when dockerizing this utility. Another stretch goal should
    be to list the filename of the zip file extracted and the date of the
    extraction to keep track of which date ranges have been loaded into the
    database. This likely should be moved to the LOAD step as part of the
    cleanup and data integrity operations step.
"""


def construct_element_dict(
        elements: list
) -> dict:
    all_tags = {}

    for element in elements:
        if element.text not in all_tags.keys():
            all_tags[element.text] = element

    return all_tags


def main():

    # TODO: Remove these paths to secrets file.
    source_data_url = 'https://divvy-tripdata.s3.amazonaws.com/index.html'
    download_path = 'c:\\Users\\steph\\Documents\\Lambda Projects\\Lambda-Comp-Sci\\python_projects\\data_pipeline\\source_files\\'
    download_directory = create_dated_directory(download_path)

    # # Database Connection Details
    # TODO: Remove testing DB details
    username = 'sa'
    password = 'D0nkeyK0ng!'
    host = 'localhost'
    port = 1433
    database = 'divvy'

    print("----- Setting up Selenium driver (Firefox)")
    driver = setup_driver(
        parent_download_dir=download_path
    )
    driver.get(source_data_url)
    print("----- Successfully requested data from: ", "\n\t", source_data_url)

    engine = configure_sqlalchemy_conn(username=username,
                                       password=password,
                                       host=host,
                                       port=port,
                                       database=database)

    print("----- Database Engine configured")
    # We take a sample from the information schema to see if
    # the table exists. If it does, we'll just append our
    # results to it. If it doesn't, though, we create it.
    # sql_text = sa.text(
    #     """
    #         SELECT *
    #         FROM INFORMATION_SCHEMA.TABLES
    #         WHERE TABLE_NAME = 'raw.divvy_trips'
    #     """)

    # retrieved_tables = sa.text(
    #     """
    #         SELECT name
    #         FROM [divvy].[dbo].[ingested_files]
    #     """)

    wait = WebDriverWait(driver, 2)
    wait.until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "a")
        )
    )

    new_tags = set()
    all_tags = construct_element_dict(
        driver.find_elements(By.CSS_SELECTOR, value='a'))

    with engine.begin() as conn:
        # Populate the set of new_tags with all of the distinct tags that have not already been downloaded.
        names = conn.execute(ingested_tables).fetchall()
        for tag_name in all_tags.keys():
            if (tag_name in names[0]) or (tag_name == 'index.html'):
                print("Omitted invalid tag: ", tag_name)
                continue
            else:
                new_tags.add(tag_name)

        # For each of the tags within the new_tags set, click it and download it.
        try:
            for tag_name in new_tags:
                print(tag_name)
                if "-divvy-tripdata" in tag_name:
                    print("Clicking on ", tag_name)
                    all_tags[tag_name].click()
                    time.sleep(10)
        except Exception:
            raise Exception
        finally:
            driver.quit()
            print("----- Successfully exited!")

    print("----- File(s) downloaded. Extracting...")

    # extract all of the downloaded files to .csv files
    for file in os.listdir(download_directory):

        filepath = download_directory + file
        try:
            with zipfile.ZipFile(filepath, 'r') as file_ref:
                file_ref.extractall(download_directory)
                print(f"{file} extracted to {download_directory}.")
        except FileNotFoundError as e:
            print(f'The file was not found: {e}')
            raise e


if __name__ == '__main__':
    main()

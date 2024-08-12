import os
import time
import typing
import zipfile
import datetime
import polars as pl
from dotenv import load_dotenv
import sqlalchemy as sa
from utils import (
    configure_sqlalchemy_conn,
    construct_element_dict,
    create_dated_directory,
    setup_driver
)
from playwright.sync_api import Page, Playwright, sync_playwright
from sql_queries import queries

load_dotenv('/home/stephen-tanksley/Documents/Code/Python_Projects/divvy_project/api/.env')

"""
    EXTRACTION

    This module focuses entirely on the process of extracting data from 
    where it is hosted online into a format which can be loaded into a 
    database. The URL https://divvy-tripdata.s3.amazonaws.com/index.html 
    contains links to a number of .CSV files which contain ridership data 
    for the Divvy bike system in Chicago.

    A stretch goal for this module is to use an environment variable to specify
    the working directory for the downloaded file. This would be useful for 
    later when dockerizing this utility. Another stretch goal should
    be to list the filename of the zip file extracted and the date of the
    extraction to keep track of which date ranges have been loaded into the
    database. This likely should be moved to the LOAD step as part of the
    cleanup and data integrity operations step.
"""

def scrape_links_from_page(
            playwright: Playwright,
            download_path: str = None, 
            source_url: str = None,
            ingested_files: list = None
        ) -> None:
        # Input a page, output is None, files are created
        firefox = playwright.firefox
        browser = firefox.launch()
        page = browser.new_page()
        page.goto(source_url, wait_until='domcontentloaded')
        time.sleep(1)

        content = page.get_by_role('link').all()
        links = [link for link in content if '-divvy-tripdata' in link.inner_text()]
        links = [link for link in links if link.inner_text().split(".")[0] not in ingested_files]
        
        for link in links:
            filename = link.inner_text()
            print(f"----- Accessing {filename} -----")
            with page.expect_download() as download_info:
                page.get_by_text(filename).click()
                download = download_info.value
            
            download.save_as(f"{download_path}/{download.suggested_filename}")
            print(f"Download saved to: {download_path}/{download.suggested_filename}", "\n")
            time.sleep(1)

        browser.close()

        return 


def main():

    source_data_url = os.getenv('SOURCE_DATA_URL')
    download_path = os.getenv('DOWNLOAD_PATH')

    host = os.getenv('DB_HOST')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    database = os.getenv('DB_DATABASE_NAME')  

    print(host, username, password, database)


    download_directory = create_dated_directory(download_path)
    os.chmod(download_directory, 0o744)

    print("----- Download directory configured -----")
    print(download_directory, '\n')
    engine = configure_sqlalchemy_conn(username=username,
                                       password=password,
                                       host=host,
                                       database=database,
                                       db_engine='postgresql')

    print("----- Database Engine configured -----")
    print(engine.url, '\n')

    ingested_files = None
    with engine.begin() as conn:
        try:
            ingested_files = [file[0].split(".")[0] for file in conn.execute(queries['select_ingested_files']).fetchall()]
        except Exception as e:
            print(e)
            raise
    
    with sync_playwright() as playwright:
        scrape_links_from_page(
            playwright=playwright,
            download_path=download_directory,
            source_url=source_data_url,
            ingested_files=ingested_files
        )


    print("----- File(s) downloaded. Extracting...")

    # extract all of the downloaded files to .csv files
    for file in os.listdir(download_directory):

        filepath = f"{download_directory}/{file}"
        os.chmod(filepath, 0o744)
        try:
            with zipfile.ZipFile(filepath, 'r') as file_ref:
                file_ref.extractall(download_directory)
                print(f"{file} extracted to {download_directory}", '\n')
        except FileNotFoundError as e:
            print(f'The file was not found: {e}', '\n')
            raise e


if __name__ == '__main__':
    main()
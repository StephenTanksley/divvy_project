import sys
import os
import uvicorn
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import sqlalchemy as sa
from sqlalchemy import select, text
# from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sql_queries import queries
from utils import configure_sqlalchemy_conn
from datetime import datetime
app = FastAPI()

origins = ['http://localhost:3000']

load_dotenv(f'{os.getcwd()}\\data_capture\\.env')

# load_dotenv('C:\\Users\\steph\\Documents\\Lambda Projects\\Lambda-Comp-Sci\\python_projects\\data_pipeline\\data_capture\\.env')

# Async sessionmaking.
# https://www.youtube.com/watch?v=cH0immwfykI

username = os.getenv('DB_USERNAME') or 'postgres'
password = os.getenv('DB_PASSWORD') or 'D0nkeyK0ng!'
host = os.getenv('DB_HOST_DOCKER') or '127.0.0.1'
port = os.getenv('DB_PORT') or 5432
database = os.getenv('DB_NAME') or 'divvy'

engine = configure_sqlalchemy_conn(username=username,
                                   password=password,
                                   host=host,
                                   database=database,
                                   db_engine='postgresql'
                                   )
engine_url = engine.url

# Create a Session
Session = sessionmaker(engine)


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


def create_hour_range(min_hour: int = None, max_hour: int = None):
    # each pair in `pairs` represents a BETWEEN value in the SQL database.
    pairs = []
    current = min_hour
    while current < max_hour:
        pairs.append((current, current + 1))
        current += 1

    return pairs


with Session() as session:

    @app.get('/')
    async def root():
        return {"message": "Hello world!"}

    @app.get('/files')
    async def get_ingested_files():
        with session.begin():
            print(f"{datetime.now()} - Retrieving ingested files...")
            try:
                start = datetime.now()
                files = session.execute(
                    queries['select_ingested_files']).mappings().all()
                stop = datetime.now()
                print(stop - start)
                return files
            except Exception as e:
                print(repr(e))
                print("Oops")
                raise

# What if we were to overfetch a little bit at the start by filtering just on date?
# At that point, we'd be able to do client-side filtering using cached data.
# That should get us a mix of speed and relative simplicity in fetching.
# If I prompt people for the definitions of what they want,

# TODO: This seems like a bit of premature optimization. I don't specifically need to do this.
# What if, when I send a GET request and I pass in a time_start and end_hour value, I use that to construct a range
# i.e. time_start = 7, end_hour = 10 ----> (7-8), (8-9), (9-10). Then I'd be able to parallelize these requests
# to speed up data fetching.

    # The /trips endpoint assumes you want to fetch all the data for an entire day for the whole system.
    @app.get('/trips')
    async def get_trips(
        year: str = '2020',
        month: str = '05',
        day: str = '01',
        start_hour: str = 0,
        end_hour: str = 24,
        min_lat: float = 41.66,
        max_lat: float = 42.16,
        min_lng: float = -87.87,
        max_lng: float = -87.53
    ):


        with session.begin():
            print(
                f"{datetime.now()} - Retrieving trips...")
            try:
                start = datetime.now()
                trips = session\
                    .execute(
                        queries['select_raw_trips']
                        .bindparams(
                            year=year,
                            month=month,
                            day=day,
                            start_hour=start_hour,
                            end_hour=end_hour,
                            min_lat=min_lat,
                            min_lng=min_lng,
                            max_lat=max_lat,
                            max_lng=max_lng
                        ))\
                    .mappings()\
                    .all()

                stop = datetime.now()
                print(datetime.now(), ' - ',
                      'Total records returned: ', len(trips))
                print(datetime.now(), ' - ',
                      'Total request duration: ', stop - start)
                return trips
            except Exception as e:
                print(repr(e))
                raise

"""
    WEB API DESIGN:

    Only GET and parameterized GET requests should be required. All database-level transformations should be handled by the transform/load steps. 

    GET DBO TRIPS - queries['select_divvy_trips_dbo'], these represent trips in the finished, prod version of the data model.

    GET RAW TRIPS - queries['select_divvy_trips_raw'], these represent trips exactly as I get them from the CSV file.

    ex: 
        https://127.0.0.1:8000/trips - GET Trips
        https://127.0.0.1:8000/trips?

    https://elements.heroku.com/addons/heroku-postgresql#details
    Heroku might be the play here. At the Essential 2 level,
    I'd be spending about $20/month on this app in storage.

    What dimensions can I slice the analytics on?

    1) Date (specific day / window)
    2) Time of day (window)
    3) Type of bike (electric or pedal powered)
    4) lat/lon coordinate grid.
"""

if __name__ == '__main__':
    uvicorn.run("main:app",
                port=5000,
                log_level='info',
                reload=True
                )
    # print(create_hour_range(min_hour=3, max_hour=10))

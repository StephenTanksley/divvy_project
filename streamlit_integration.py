import os
import streamlit as st
import pandas as pd
import numpy as np
import sqlalchemy as sa
from utils import configure_sqlalchemy_conn
from datetime import datetime, time, timedelta
import sys
from dotenv import load_dotenv

# load_dotenv(f'{os.getcwd()}{os.sep}data_capture{os.sep}.env')
load_dotenv('/home/stephen-tanksley/Documents/Code/Python_Projects/divvy_project/.env')

default_sql = sa.text("""
SELECT t.*
FROM divvy.raw.trips t
INNER JOIN divvy.raw.stations ss ON t.start_station_id = ss.station_id
INNER JOIN divvy.raw.stations es ON t.end_station_id = es.station_id
WHERE source_file_id = 1
AND DATE_PART('day', t.started_at) = 20
""")

all_years_query = sa.text("""
SELECT DISTINCT
    DATE_PART('year', started_at)
FROM divvy.raw.trips
""")

max_date_query = """
SELECT MAX(t.ended_at) as max_date
FROM divvy.raw.trips t;
"""


def format_sql(
        year: int=None, 
        month: int=None, 
        day: int=None,
        hour_start: int=None,
        hour_end: int=None
        ):
    query_builder = """
SELECT 
    t.ride_id, 
    t.rideable_type, 
    t.started_at, 
    t.ended_at,
    ss.station_name as start_station_name, 
    ss.station_id as start_station_id, 
    ss.lat as lat, 
    ss.lng as lon, 
    es.station_name as end_station_name, 
    es.station_id as end_station_id, 
    es.lat as LATITUDE, 
    es.lng as LONGITUDE
FROM divvy.raw.trips t
INNER JOIN divvy.raw.stations ss ON t.start_station_id = ss.station_id
INNER JOIN divvy.raw.stations es ON t.end_station_id = es.station_id
"""

    if year is not None:
        query_builder += f"WHERE DATE_PART('year', t.started_at) = {year}"

    if month is not None:
        query_builder += f"""
        AND DATE_PART('month', t.started_at) = {month}"""

    if day is not None:
        query_builder += f"""
         AND DATE_PART('day', t.started_at) = {day}"""
        
    if hour_start is not None and hour_end is not None:
        query_builder += f"""
         AND DATE_PART('hour', t.started_at) BETWEEN {hour_start} AND {hour_end}"""
        
    return query_builder


@st.cache_data
def fetch_dataframe(
        sql: str = None, 
        _engine: sa.Engine = None,
        ) -> pd.DataFrame:
    df = pd.read_sql(sql=sql, con=_engine)
    return df


def main():
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    hostname = os.getenv('DB_HOST')
    database = os.getenv('DB_NAME')

    _engine = configure_sqlalchemy_conn(
        username=username,
        password=password,
        database=database,
        host=hostname,
        db_engine='postgresql'
    )
    max_date_df = fetch_dataframe(sql=max_date_query, _engine=_engine)
    max_date = max_date_df['max_date'][0]

    default_day = datetime(year=2020, month=4, day=20)
    default_start_time = time(hour=0, minute=0)
    default_end_time = time(hour=23, minute=59)

    st.sidebar.write("**Date/Time Input**")
    with st.sidebar:
        with st.expander('Configuration Sidebar', icon=":material/calendar_clock:") as expander:

            date_value = st.sidebar.date_input(
                label="Date Input", 
                value=default_day, 
                min_value=datetime(
                    year=2020, 
                    month=4, 
                    day=1
                ),
                max_value=datetime(
                    year=max_date.year, 
                    month=max_date.month, 
                    day=max_date.day
                    )
                )
            month_checkbox = st.sidebar.checkbox("Filter by month", value=True)
            day_checkbox = st.sidebar.checkbox("Filter by day", value=True)
            time_checkbox = st.sidebar.checkbox("Filter by hour range", value=True)
            
            default_min_time = time(hour=0, minute=0)
            default_max_time = time(hour=23, minute=0)

            if time_checkbox:
                time_slider = st.sidebar.slider(
                    label="Time Input Slider",
                    min_value=default_min_time,
                    max_value=default_max_time,
                    value=[default_start_time, default_end_time],
                    step=timedelta(minutes=30)
                    )
                
                start_time = time_slider[0]
                end_time = time_slider[-1]

            query = format_sql(
                year=date_value.year, 
                month=date_value.month if month_checkbox else None, 
                day=date_value.day if day_checkbox else None, 
                hour_start=start_time.hour if time_checkbox else None,
                hour_end=end_time.hour if time_checkbox else None
                )

    with st.spinner("Be right there...huffing and puffing..."):
        df = fetch_dataframe(sql=query, _engine=_engine)
    
    
    st.markdown(body="""
                
    ## Bikes are pretty awesome!
                
    There are numerous benefits to riding bicycles instead of driving cars. 
    They're cleaner, quieter and safer than cars, far less likely to harm pedestrians, 
    don't produce anywhere near as much microplastics from their tires and they're fun to boot! 
    In this data exploration environment, we'll take a look at three distinct pieces to this:

    1) Bicycles are great for the environment
    2) Bicycles are less expensive than car ownership
    3) Bike riding is great for your health
    """
                )
    col1, col2 = st.columns(2)

    time_range_text = f"between {start_time.strftime("%H:%M") if time_checkbox else default_min_time} and {end_time.strftime("%H:%M") if time_checkbox else default_max_time}"

    environment_text = f"""
    ### :material/directions_bike: - The Environment
    
    For this first section, let's just look at a sampling of some environmental impacts from bike transit
    to see what a difference it makes. Let's take a look at some of the impacts of riding bikes based on the data you chose! 
    For the window you selected ( {date_value} {time_range_text if time_checkbox else ""} ), there were a total of {len(df)} trips.
    Riders during this window rode for a grand total of <TODO: Add miles> miles. Bear in mind - this is only the trips recorded by a bikeshare service.
    This doesn't even represent all cyclists who use cycling as their primary mode of transportation.
    
    Let's outline two scenarios - one where only some of these trips are replacing car trips and another where all reported trips are replacing car trips.

    **Scenario 1**: A very conservative number of bicycle riders are replacing their car trips (25%) with bike trips instead.

    **Scenario 2**: All of the bicycle riders are replacing their car trips (100%) with bike trips instead.
    """

    financial_text = f"""
    ### :material/directions_bike: - The Money

    """
    col1.markdown(body=environment_text)
    # col2.dataframe(data=df)
    col2.map(data=df, color='#4578e7', size=.4)

    

if __name__ == '__main__':
    main()

    # https://www.epa.gov/greenvehicles/greenhouse-gas-emissions-typical-passenger-vehicle
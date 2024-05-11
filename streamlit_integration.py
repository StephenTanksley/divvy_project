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
SELECT 
    t.ride_id,
    t.start_station_id,
    t.started_at,
    t.end_station_id,
    t.ended_at,
    sd.distance_in_meters
FROM divvy.raw.trips t
INNER JOIN divvy.raw.stations ss ON t.start_station_id = ss.station_id
INNER JOIN divvy.raw.stations es ON t.end_station_id = es.station_id
INNER JOIN divvy.raw.station_distances sd
                                 ON t.start_station_id = sd.station_1 
                                AND t.end_station_id   = sd.station_2
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
    es.lng as LONGITUDE,
    sd.distance_in_meters
FROM divvy.raw.trips t
INNER JOIN divvy.raw.stations ss ON t.start_station_id = ss.station_id
INNER JOIN divvy.raw.stations es ON t.end_station_id = es.station_id
INNER JOIN divvy.raw.station_distances sd ON t.start_station_id = sd.station_1 AND t.end_station_id = sd.station_2
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

    with st.spinner("Be right there..."):
        df = fetch_dataframe(sql=query, _engine=_engine)
    
    
    st.markdown(body="""
    ## Bikes are pretty awesome!
                
    There are numerous benefits to riding bicycles instead of driving cars. 
    They're cleaner, quieter and safer than cars, far less likely to harm pedestrians, 
    don't produce anywhere near as much microplastics from their tires and they're fun to boot! 
    In this data exploration environment, we'll take a look at two distinct pieces to this:

    - [Bicycles are great for the environment](#environment)
    - [Bike riding is great for your health](#health)
    """)

    col1, col2 = st.columns(2)

    miles = round(sum(df['distance_in_meters']) / 1609.344, 2)

    football_field_dimensions = 360 * 160 * 144 # 360 feet long by 160 feet wide multiplied by 144 to turn square feet into square inches.

    ford_f_150_dimensions = 209.1 * 79.9 # square inches

    # time_range_text = f"between {start_time.strftime("%H:%M") if time_checkbox else default_min_time} and {end_time.strftime("%H:%M") if time_checkbox else default_max_time}"

    environment_text_1 = f"""
    ### :material/directions_bike: :material/directions_bike: :material/directions_bike:
    ## Environment
    ### :material/directions_bike: :material/directions_bike: :material/directions_bike:

    For this first section, let's just look at a sampling of some environmental impacts from bike transit
    to see what a difference it makes.
    
    In the selected window, there were a total of {len(df)} trips.
    Riders during this window rode for a grand total of {miles} miles. 
    Bear in mind - this is only the trips recorded by a bikeshare service.
    This doesn't represent all cyclists (i.e. those who use cycling as their primary mode of transportation and own their bicycle)
    
    Let's imagine each one of those trips were car trips instead.
    """

    environment_text_2 = f"""
    In a hypothetical scenario, let's imagine that we've got the most popular vehicle choice in Chicago for each one of those trips - the Ford F-150. 
    A Ford F-150 truck measures about 209.1" x 79.9". If we were to park as many Ford F-150 trucks as we could reasonably manage into the space of a football field,
    you'd need {round(len(df) * ford_f_150_dimensions / football_field_dimensions, 2)} football fields to hold them all.
    When you consider the amount of space that these vehicles take up on the streets, you quickly come to realize that a huge amount of physical space is just taken up
    by the vehicles themselves.

    The EPA estimates that the average vehicle emits 400 grams of CO$_2$ per mile. Therefore the total amount saved during the selected window is approximately {400 * miles} grams.
    Once we convert to kilograms, we have a grant total of emissions savings of around {round((400 * miles) / 1000, 2)} kg. 
    """

    col1.markdown(body=environment_text_1)
    # col2.dataframe(data=df)
    col2.map(data=df, color='#4578e7', size=.4)

    st.markdown(body=environment_text_2)


    financial_text = f"""
    ## Health :material/directions_bike:

    We're told to get active every day in order to keep weight off and keep hearts healthy, but 

    """


if __name__ == '__main__':
    main()



    """
    TODO: This whole page needs fixing. Long story short - I want to make the case that 
        1) Bicycles are better for the environment
        2) Bicycles are cheaper than car ownership
        3) Bike riding is great for your health
        4) Bike infrastructure types
        5) Bike infrastructure advocacy
        6) What to do for Chicago natives (include the utility to look up local aldermen and contact them to advocate for bicycle infrastructure and reclaiming streets)
    """
    # https://www.bts.gov/topics/bicycle-and-pedestrian-travel - bicycle and pedestrian travel statistics from the government
    # https://www.epa.gov/greenvehicles/greenhouse-gas-emissions-typical-passenger-vehicle
    # https://www.epa.gov/energy/greenhouse-gas-equivalencies-calculator#results
    # https://www.epa.gov/greenvehicles/greenhouse-gas-emissions-typical-passenger-vehicle -- average CO2 emitted by a typical passenger vehicle.
    # https://chicago.councilmatic.org/person/martin-matthew-j-54f1707f5a16/?view=donations
    # https://www.chicago.gov/city/en/depts/mayor/provdrs/your_ward_and_alderman/svcs/find_my_alderman.html
    # https://www.fhwa.dot.gov/tpm/guidance/avo_factors.pdf - average vehicle occupancy for trips is 1.7. I could just round this up to 2 to be generous.
    # https://www.procyclingcoaching.com/resources/carbon-emissions-offset-calculator
    # https://www.intechopen.com/chapters/71662
    # https://www.thedrive.com/news/tire-dust-makes-up-the-majority-of-ocean-microplastics-study-finds
    # https://pmc.ncbi.nlm.nih.gov/articles/PMC10546027/#s3


    
    """ There's a few things to talk about here:
            There's the environmental impact of the over-reliance on roads and car-based infrastructure. 
            There's the effects of converting so much green space to car space. 
            There's also an argument to be made for the effect that car-focused infrastructure has on splitting up communities. 
            Not to mention microplastics in the environment. 
            The thing to point out here is that our choice of transit has not just direct, first-order effects, but plenty of downstream ramifications as well.
    """
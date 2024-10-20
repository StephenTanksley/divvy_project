import streamlit as st
import pandas as pd
import numpy as np
import sqlalchemy as sa
from utils import configure_sqlalchemy_conn
from datetime import datetime, time


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
    t.id,
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
        # query_builder += f"WHERE DATE_PART('year', t.started_at) = :year"
        query_builder += f"WHERE DATE_PART('year', t.started_at) = {year}"

        # all_params.append(bindparam(key='year', value=year, type_=int))

    if month is not None:
        query_builder += f"""
        AND DATE_PART('month', t.started_at) = {month}"""

        # AND DATE_PART('month', t.started_at) = :month"""
        # all_params.append(bindparam(key='month', value=month, type_=int))


    if day is not None:
        query_builder += f"""
         AND DATE_PART('day', t.started_at) = {day}"""
        # AND DATE_PART('day', t.started_at) = :day"""

        # all_params.append(bindparam(key='day', value=day, type_=int))
# 
        
    if hour_start is not None and hour_end is not None:
        query_builder += f"""
         AND DATE_PART('hour', t.started_at) BETWEEN {hour_start} AND {hour_end}"""
        
        #  AND DATE_PART('hour', t.started_at) BETWEEN :hour_start AND :hour_end"""

        # all_params.append(bindparam(key='hour_start', value=hour_start, type_=int))
        # all_params.append(bindparam(key='hour_end', value=hour_end, type_=int))


    # formatted_query = sa.text(query_builder)
    # for param in all_params:
    #     formatted_query.bindparams(param)

    # return formatted_query
    return query_builder


@st.cache_data
def fetch_dataframe(
        sql: str = None, 
        _engine: sa.Engine = None,
        ) -> pd.DataFrame:
    df = pd.read_sql(sql=sql, con=_engine)
    return df


def main():
    hostname = 'localhost' or 'host.docker.internal'
    username = 'postgres'
    password = 'D0nkeyK0ng!'
    database = 'divvy'

    _engine = configure_sqlalchemy_conn(
        username=username,
        password=password,
        database=database,
        host=hostname,
        db_engine='postgresql'
    )
    max_date = fetch_dataframe(sql=max_date_query, _engine=_engine)
    print(max_date)
    default_day = datetime(year=2020, month=4, day=20)
    default_start_time = time(hour=6, minute=0)
    default_end_time = time(hour=10, minute=0)

    print(default_start_time)

    st.sidebar.write("This is a sidebar!")
    with st.sidebar:
        with st.expander('Configuration Sidebar'):
            date_value = st.sidebar.date_input(label="Date Input", value=default_day, min_value=datetime(year=2020, month=4, day=1))
            start_time_value = st.sidebar.time_input(label="Start Time", step=1800, value=default_start_time)
            end_time_value = st.sidebar.time_input(label="End Time", step=1800, value=default_end_time)

            if start_time_value > end_time_value or end_time_value < start_time_value:
                st.error("Please select a start time that is before the end time.")
            query = format_sql(
                year=date_value.year, 
                month=date_value.month, 
                day=date_value.day, 
                hour_start=start_time_value.hour, 
                hour_end=end_time_value.hour
                )

            df = fetch_dataframe(sql=query, _engine=_engine)

    
    st.dataframe(data=df)
    st.map(data=df, color='#4578e7', size=.5)


if __name__ == '__main__':
    main()
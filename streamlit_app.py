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

    
    query_builder = f"""
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
    - [Bicycles make a lot of good financial sense](#finance)
    - [Bike riding is great for your health](#health)
    """)

    col1, col2 = st.columns(2)

    miles = round(sum(df['distance_in_meters']) / 1609.344, 2)

    football_field_dimensions = 360 * 160 * 144 # 360 feet long by 160 feet wide multiplied by 144 to turn square feet into square inches.

    ford_f_150_dimensions = 209.1 * 79.9 # square inches

    # time_range_text = f"between {start_time.strftime("%H:%M") if time_checkbox else default_min_time} and {end_time.strftime("%H:%M") if time_checkbox else default_max_time}"

    environment_text_1 = f"""
    ## Environment
    ---
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

    financial_data = {
        'average_salary': 59384.00,
        'average_car_cost_annual': 12297.00,
        'annual_divvy_membership': 143.00,
        'annual_all_access_metra_pass': 1980.00
    }

    financial_text = f"""## Finance
---

Let's face it: Cars are expensive. At a time when the average American would struggle to field a surprise $500 expense, a car represents a very hefty price for admission to the American dream.
Due to the way that we've mostly built our cities and suburbs (low to medium density with lots of single-family zoned residential areas), we rely on our cars to get around unless we happen to live in a city with a good transit system.

A few facts:
1. [Investopedia](https://www.investopedia.com/should-you-ditch-your-second-car-in-retirement-the-surprising-savings-of-downsizing-11734592, "Should you ditch your second car in retirement?") points out that the average cost of owning a new vehicle is about \$12,297.00 per year. 
This figure represents a new vehicle which is paid for with financing, so if you purchased your vehicle with cash or the vehicle is already paid off, this figure would be lower.
2. [USA Today](https://www.usatoday.com/money/blueprint/business/hr-payroll/average-salary-us/#source, "Average salary in the U.S. in 2024") reports that the average income for a worker in the United States is about \$59,384.00. 
This figure represents the national average and does not discriminate for factors like age, race, gender or profession.3) If we run the numbers on these figures, we can see that in our average scenario, \$12,297.00 is approximately 20.71% of \$59,384.00  
3. By way of contrast, a Divvy bike membership costs \$143.90 billed up front to cover a whole year of riding. A Divvy membership for a whole year costs only {round((financial_data['annual_divvy_membership'] / financial_data['average_salary']) * 100, 2)}% of the above-mentioned average salary of \$59,384.00 as opposed to 20.71%
4. If you were commuting in from the suburbs and needed transit options, Chicago's Metra service in particular offers an exceptional value with a Metra Monthly Pass + Regional Connect option. 
At its maximum cost connecting all Metra zones and all lines within the city, this pass would enable a rider to take any bus (CTA and PACE), any train (Metra and CTA) anywhere within the Chicago metro area (including suburbs) for about \$1,980.00. 
If you want to be cheeky, add Divvy for \${financial_data['annual_divvy_membership']} for a grand total of \${financial_data['annual_all_access_metra_pass'] + financial_data['annual_divvy_membership']} to get around the city and suburbs, never need to park, never need gas, etc. Divided by 12 months per year, you're only looking at paying \$176.92 per month this way. 
It sounds like a lot, but when you take into consideration that you wouldn't need to pay for parking, gas, insurance, maintenance, license, title, the long term savings add up.
"""

    st.markdown(body=financial_text)


if __name__ == '__main__':
    main()

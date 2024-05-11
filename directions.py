import json
import polars as pl
from utils import configure_sqlalchemy_conn
import requests

# def load_adjacency_list():
#     with open('station_id_adjacency.json', 'r') as file:
#         dep_graph = json.load(file)
#     return dep_graph


def create_pairs_df():
    pairs_df = pl.read_csv('station_pairs.csv', has_header=True)
    pairs_df = pairs_df.select(['start_station_id', 'end_station_id'])
    return pairs_df


async def get_mapbox_directions(start_lat: float = None,
                                start_lng: float = None,
                                end_lat: float = None,
                                end_lng: float = None,
                                access_token: str = None):
    """
    The purpose of this function is to define an async function to send
    GET requests to Mapbox to retrieve step by step directions for the map.
    Access token is the public access token from Mapbox.
    """
    return f"https://api.mapbox.com/directions/v5/mapbox/cycling/{start_lng}%2C{start_lat}%3B{end_lng}%2C{end_lat}?alternatives=false&continue_straight=true&geometries=geojson&language=en&overview=simplified&steps=true&access_token={access_token}"


if __name__ == '__main__':
    pairs = create_pairs_df()
    engine = configure_sqlalchemy_conn(username=username,
                                       password=password,
                                       host=host,
                                       port=port,
                                       database=database)

    pairs.write_database('raw.station_directions',
                         connection=engine.url, if_exists='replace')

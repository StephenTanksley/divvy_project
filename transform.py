import os
import sys
from sql_queries import queries
from datetime import datetime
from dotenv import load_dotenv
import polars as pl
from sqlalchemy.orm import sessionmaker
from utils import configure_sqlalchemy_conn, check_station_ids

# Load environment variables
load_dotenv('/home/stephen-tanksley/Documents/Code/Python_Projects/divvy_project/.env')

# TODO: Refactor the engine to instead be configured as a Session. Use session style for transactions.
# https://docs.sqlalchemy.org/en/20/orm/session_basics.html#what-does-the-session-do


class Transformer:
    def __init__(
        self, 
        csv_filepath: str = None, 
        source_file_id: int = -1
        ):
        self._csv_filepath = csv_filepath
        self._now = datetime.now()
        self._source_file = self._csv_filepath.rsplit(
            '/', 1)[-1] if self._csv_filepath is not None else None
        self._df = pl.read_csv(source=self._csv_filepath,
                               has_header=True,
                               try_parse_dates=True,
                               infer_schema_length=10000)
        # self._source_file_id = source_file_id
        self._stations_df = None


    def enrich_for_raw_layer(self, raw_table_exists=True, conn_url: str = None):
        self._df = self._df.with_columns(
            created_on=pl.lit(datetime.now()),
            updated_on=pl.lit(datetime.now()),
            source_file_id=pl.lit(self._source_file_id)
        )
        # self._df = self._df.drop_nulls()
        self._df = self._df.drop(['start_station_name', 'start_lat', 'start_lng', 'end_station_name', 'end_lat', 'end_lng'])
        self._df.write_database('raw.trips',
                                connection=conn_url,
                                if_table_exists='append'
                                if raw_table_exists
                                else 'replace',
                                )


    def enrich_for_staging_layer(self, conn_url: str = None):
        """
            When writing to the staging layer, I'm beginning to add
            enriched columns to the source. I think loading into a smaller
            final table is going to lead to faster load times for users.

            Accepts a conn object which is the opened connection that
            can be used to send SQL queries through to the database.
            TODO: Might need to be swapped to a Session. TBD.
        """
        df = self._df.clone().drop_nulls()

        df = df.with_columns(
            trip_length=pl.lit(df['ended_at'] - df['started_at']),
        )

        df.write_database('staging.trips',
                          connection=conn_url,
                          if_table_exists='append'
                          )
    

    def extract_stations(self, conn_url: str = None):
        # 1) Grab station_id values.
        station_ids = check_station_ids(conn_url)
        print(station_ids)
        # 2) Drop all columns except station_id, station_name, lat, lng
        new_df = self._df.select("station_id", "station_name", "lat", "lng")
        print(new_df)
    
        


if __name__ == '__main__':
    data_dir = '/home/stephen-tanksley/Documents/Source_Data'
    csvs = []
    username = os.getenv('DB_USERNAME') or 'postgres'
    password = os.getenv('DB_PASSWORD') or 'D0nkeyK0ng!'
    host = os.getenv('DB_HOST') or '127.0.0.1'
    database = os.getenv('DB_NAME') or 'divvy'

    engine = configure_sqlalchemy_conn(username=username,
                                       password=password,
                                       host=host,
                                       database=database,
                                       db_engine='postgresql')
    engine_url = engine

    # Create a Session
    Session = sessionmaker(engine)


    # Grab items from the project directory that have been unzipped to .csv files.

    for item in os.listdir(data_dir):
        if item.endswith('.txt'):
            continue
        new_dir = os.path.join(data_dir, item)
        for file in os.listdir(new_dir):
            if file.endswith('.csv'):
                csvs.append(os.path.join(new_dir, file))

    with Session() as session:
        with session.begin():
            ingested_files = [item[-1] for item in session.execute(
                    queries['select_ingested_files']).fetchall()]

            # all_current_stations =
            
    missing_files = []
    for file in csvs:
        filename = file.rsplit("/", maxsplit=1)[-1]
        if filename not in ingested_files:
            missing_files.append(file)

    # Get the list of files which have already been ingested
    for file in sorted(missing_files):
        filename = file.rsplit("/", maxsplit=1)[-1]
        source_file_id = None
        
        with Session() as session:
            with session.begin():
                ingested_files = [item[-1] for item in session.execute(
                        queries['select_ingested_files']).fetchall()]
                
                if filename not in ingested_files:
                    print(
                        f'{datetime.now()} - {file} not in dbo.ingested_files! Adding...')

                    # Insert the filename into dbo.ingested_files.
                    # This will generate an id number
                    try:
                        session.execute(
                            queries['insert_ingested_file'].bindparams(source_file=filename))
                    except Exception as e:
                        print("There was a problem: ", e)
                        session.rollback()
                    else:
                        session.commit()

            if file not in ingested_files:
                # second transaction - transformation
                # with engine.connect() as conn:
                with session.begin():
                    # Execute this line to query that same table and retrieve the
                    # id that we just generated.
                    source_file_id = session.execute(
                        queries['select_ingested_files_by_filename'].bindparams(filename=filename)).first()[0]
                    
                    print(f"{datetime.now()} - Creating dataframe for file_id :{source_file_id} - {filename}")
                    raw_table_exists = True \
                        if len(session.execute(
                            queries['select_raw_divvy_trips']).fetchone()) > 0 \
                        else False

                    transformer = Transformer(
                        file,
                        source_file_id)

                    try:
                        print(
                            f"{datetime.now()} - Writing to [divvy].[raw].[divvy_trips]")
                        # TODO: There's an error where we're trying to insert into the trips table which does not have some deprecated columns. Fix this.
                        transformer.enrich_for_raw_layer(
                            raw_table_exists=raw_table_exists,
                            conn_url=engine_url)
                    except Exception as e:
                        print("There was a problem: ", e)
                        session.execute(queries['delete_ingested_files_by_file_id'].bindparams(
                            source_file_id=source_file_id))
                        session.rollback()
                    else:
                        print(
                            f"{datetime.now()} - {file}'s trips added to raw.divvy_trips")
                        session.commit()

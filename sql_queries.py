import sqlalchemy as sa

queries = {
    'select_ingested_files': sa.text(
        """
            SELECT id, filename
            FROM [divvy].[dbo].[ingested_files]
        """
    ),

    'insert_ingested_file': sa.text(
        """
            INSERT INTO [divvy].[dbo].[ingested_files](filename)
            VALUES (:source_file);
        """
    ),

    'select_ingested_files_by_file_id': sa.text(
        """
            SELECT id
            FROM [divvy].[dbo].[ingested_files]
            WHERE filename = (:filename);
        """
    ),

    'delete_ingested_files_by_file_id': sa.text(
        """
            ;WITH data AS (
                SELECT *
                FROM [divvy].[dbo].[ingested_files]
                WHERE id = (:source_file_id)
            )
            DELETE FROM data;
        """
    ),

    'select_raw_divvy_trips': sa.text(
        """
            SELECT *
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'divvy_trips'
            AND TABLE_SCHEMA = 'raw';
        """
    ),

    'select_from_raw_view': sa.text(
        """
            SELECT         
                id,
                ride_id, 
                rideable_type, 
                started_at, 
                ended_at,
                start_station_name, 
                start_station_id, 
                end_station_name, 
                end_station_id, 
                start_lat, 
                start_lng, 
                end_lat, 
                end_lng, 
                member_casual
            FROM [divvy].[raw].[v_divvy_trips]
        """
    ),

    'select_top_200_trips': sa.text(
        """
            SELECT TOP 200
                id,
                ride_id, 
                rideable_type, 
                started_at, 
                ended_at,
                start_station_name, 
                start_station_id, 
                end_station_name, 
                end_station_id, 
                start_lat, 
                start_lng, 
                end_lat, 
                end_lng, 
                member_casual
            FROM [divvy].[dbo].[divvy_trips]
        """
    ),
    'select_dbo_divvy_trips': sa.text(
        """
            SELECT
                id,
                ride_id, 
                rideable_type, 
                started_at, 
                ended_at,
                start_station_name, 
                start_station_id, 
                end_station_name, 
                end_station_id, 
                start_lat, 
                start_lng, 
                end_lat, 
                end_lng, 
                member_casual
            FROM [divvy].[dbo].[divvy_trips]
        """
    )
}


# TODO: Join to a stations table which will have all of the station latitudes/longitudes listed. Then find a way to cross-reference the station names to assign Divvy station IDs to the ones that came back from Google. https://account.mapbox.com/auth/signup/

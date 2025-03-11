import sqlalchemy as sa

queries = {
    'select_ingested_files': sa.text(
        """
            SELECT filename
            FROM divvy.dbo.ingested_files
        """
    ),

    'insert_ingested_file': sa.text(
        """
            INSERT INTO divvy.dbo.ingested_files(filename)
            VALUES (:source_file);
        """
    ),

    'select_ingested_files_by_filename': sa.text(
        """
            SELECT id, filename
            FROM divvy.dbo.ingested_files
            WHERE filename = (:filename);
        """
    ),
        'select_ingested_files_by_id': sa.text(
        """
            SELECT id, filename
            FROM divvy.dbo.ingested_files
            WHERE id = (:id);
        """
    ),

    'delete_ingested_files_by_file_id': sa.text(
        """
            DELETE FROM divvy.dbo.ingested_files
            WHERE id IN (
                SELECT id
                FROM divvy.dbo.ingested_files
                WHERE id = (:source_file_id)
            );
        """
    ),

    'select_raw_divvy_trips': sa.text(
        """
            SELECT *
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'trips'
            AND TABLE_SCHEMA = 'raw';
        """
    ),
    
    'select_raw_stations': sa.text(
        """
            SELECT 
                id,
                station_id,
                station_name,
                lat,
                lng
            FROM divvy.raw.stations;
        """
        ),

    'select_raw_station_ids': sa.text(
        """
            SELECT 
                station_id
            FROM divvy.raw.stations;
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
            FROM divvy.raw.v_divvy_trips
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
            FROM divvy.dbo.divvy_trips
        """
    ),
    'select_raw_trips': sa.text(
        """
            SELECT
                t.id,
                t.ride_id, 
                t.rideable_type, 
                t.started_at, 
                t.ended_at,
                ss.station_name as start_station_name, 
                ss.station_id as start_station_id, 
                ss.lat as start_lat, 
                ss.lng as start_lng, 
                es.station_name as end_station_name, 
                es.station_id as end_station_id, 
                es.lat as end_lat, 
                es.lng as end_lng, 
                t.member_casual
            FROM divvy.raw.trips t
            INNER JOIN divvy.raw.stations ss
                ON t.start_station_id = ss.station_id
            INNER JOIN divvy.raw.stations es
                ON t.end_station_id = es.station_id
            WHERE DATE_PART('hour', t.started_at) BETWEEN (:start_hour) AND (:end_hour) -- 1-24
                AND DATE_PART('year', t.started_at) = (:year)
                AND DATE_PART('month', t.started_at) = (:month)
                AND DATE_PART('day', t.started_at) = (:day)
                AND ss.lat BETWEEN (:min_lat) AND (:max_lat)
                AND ss.lng BETWEEN (:min_lng) AND (:max_lng);
        """
    ),
    'select_unique_station_info':
        """
            SELECT 
                s.start_station_id station_id, 
                s.start_station_name station_name, 
                s.start_lat lat, 
                s.start_lng lng
            FROM self s
            UNION
            SELECT 
                e.end_station_id station_id, 
                e.end_station_name station_name, 
                e.end_lat lat, 
                e.end_lng lng
            FROM self e
            """
}
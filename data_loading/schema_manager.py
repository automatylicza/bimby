from sqlalchemy import text
from loguru import logger

class SchemaManager:
    def __init__(self, engine):
        self.engine = engine

    def create_tables_if_not_exists(self):
        create_statements = [
            """
            CREATE TABLE IF NOT EXISTS public.static_data_versions (
                version_id SERIAL PRIMARY KEY,
                load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                description TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.processed_folders (
                id SERIAL PRIMARY KEY,
                folder_name VARCHAR(255) UNIQUE NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.processed_files (
                id SERIAL PRIMARY KEY,
                file_path TEXT UNIQUE NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.agency (
                agency_id TEXT NOT NULL,
                agency_name TEXT,
                agency_url TEXT,
                agency_timezone TEXT,
                agency_phone TEXT,
                agency_lang TEXT,
                version_id INT NOT NULL,
                PRIMARY KEY (agency_id, version_id),
                FOREIGN KEY (version_id) REFERENCES public.static_data_versions (version_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.feed_info (
                feed_publisher_name TEXT,
                feed_publisher_url TEXT,
                feed_lang TEXT,
                feed_start_date DATE,
                feed_end_date DATE,
                version_id INT NOT NULL,
                FOREIGN KEY (version_id) REFERENCES public.static_data_versions (version_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.stops (
                stop_id TEXT NOT NULL,
                stop_code TEXT,
                stop_name TEXT NOT NULL,
                stop_lat DOUBLE PRECISION NOT NULL,
                stop_lon DOUBLE PRECISION NOT NULL,
                zone_id TEXT,
                version_id INT NOT NULL,
                PRIMARY KEY (stop_id, version_id),
                FOREIGN KEY (version_id) REFERENCES public.static_data_versions (version_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.routes (
                route_id TEXT NOT NULL,
                agency_id TEXT NOT NULL,
                route_short_name TEXT NOT NULL,
                route_long_name TEXT,
                route_desc TEXT,
                route_type INT,
                route_color TEXT,
                route_text_color TEXT,
                version_id INT NOT NULL,
                PRIMARY KEY (route_id, version_id),
                FOREIGN KEY (agency_id, version_id) REFERENCES public.agency (agency_id, version_id),
                FOREIGN KEY (version_id) REFERENCES public.static_data_versions (version_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.calendar (
                service_id TEXT NOT NULL,
                monday INT,
                tuesday INT,
                wednesday INT,
                thursday INT,
                friday INT,
                saturday INT,
                sunday INT,
                start_date DATE,
                end_date DATE,
                version_id INT NOT NULL,
                PRIMARY KEY (service_id, version_id),
                FOREIGN KEY (version_id) REFERENCES public.static_data_versions (version_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.calendar_dates (
                service_id TEXT NOT NULL,
                date DATE NOT NULL,
                exception_type INT NOT NULL,
                version_id INT NOT NULL,
                PRIMARY KEY (service_id, date, version_id),
                FOREIGN KEY (service_id, version_id) REFERENCES public.calendar (service_id, version_id),
                FOREIGN KEY (version_id) REFERENCES public.static_data_versions (version_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.shapes (
                shape_id TEXT NOT NULL,
                shape_pt_lat DOUBLE PRECISION NOT NULL,
                shape_pt_lon DOUBLE PRECISION NOT NULL,
                shape_pt_sequence INT NOT NULL,
                version_id INT NOT NULL,
                PRIMARY KEY (shape_id, shape_pt_sequence, version_id),
                FOREIGN KEY (version_id) REFERENCES public.static_data_versions (version_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.trips (
                trip_id TEXT NOT NULL,
                route_id TEXT NOT NULL,
                service_id TEXT NOT NULL,
                trip_headsign TEXT,
                direction_id INT,
                block_id TEXT,
                shape_id TEXT,
                wheelchair_accessible INT,
                brigade INT,
                version_id INT NOT NULL,
                PRIMARY KEY (trip_id, version_id),
                FOREIGN KEY (route_id, version_id) REFERENCES public.routes (route_id, version_id),
                FOREIGN KEY (service_id, version_id) REFERENCES public.calendar (service_id, version_id),
                FOREIGN KEY (version_id) REFERENCES public.static_data_versions (version_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.stop_times (
                trip_id TEXT NOT NULL,
                arrival_time TEXT,
                departure_time TEXT,
                stop_id TEXT NOT NULL,
                stop_sequence INT NOT NULL,
                stop_headsign TEXT,
                pickup_type INT,
                drop_off_type INT,
                version_id INT NOT NULL,
                PRIMARY KEY (trip_id, stop_sequence, version_id),
                FOREIGN KEY (trip_id, version_id) REFERENCES public.trips (trip_id, version_id),
                FOREIGN KEY (stop_id, version_id) REFERENCES public.stops (stop_id, version_id),
                FOREIGN KEY (version_id) REFERENCES public.static_data_versions (version_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.trip_updates (
                entity_id TEXT,
                is_deleted BOOLEAN,
                trip_id TEXT NOT NULL,
                route_id TEXT,
                stop_sequence INT,
                arrival_delay INT,
                departure_delay INT,
                schedule_relationship INT,
                timestamp BIGINT NOT NULL,
                delay INT,
                stop_id TEXT NULL,
                start_time TEXT,
                start_date TEXT,
                version_id INT NOT NULL,
                PRIMARY KEY (trip_id, timestamp, version_id),
                FOREIGN KEY (trip_id, version_id) REFERENCES public.trips (trip_id, version_id),
                FOREIGN KEY (route_id, version_id) REFERENCES public.routes (route_id, version_id),
                FOREIGN KEY (version_id) REFERENCES public.static_data_versions (version_id),
                FOREIGN KEY (stop_id, version_id) REFERENCES public.stops (stop_id, version_id)
                ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS public.vehicle_positions (
                entity_id TEXT NOT NULL,
                is_deleted BOOLEAN,
                trip_id TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                speed DOUBLE PRECISION, 
                bearing DOUBLE PRECISION,
                occupancy_status INT,
                timestamp BIGINT NOT NULL,
                version_id INT NOT NULL,
                PRIMARY KEY (entity_id, timestamp, version_id),
                FOREIGN KEY (trip_id, version_id) REFERENCES public.trips (trip_id, version_id),
                FOREIGN KEY (version_id) REFERENCES public.static_data_versions (version_id)
            );
            """
        ]
        with self.engine.begin() as conn:
            for stmt in create_statements:
                conn.execute(text(stmt))
        logger.info("Wszystkie tabele zostały utworzone (jeśli wcześniej nie istniały).")

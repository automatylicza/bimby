import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Dict
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from loguru import logger
from etl.gtfs_realtime_pb2 import FeedMessage
from parsers.trip_update_parser import TripUpdateParser
from parsers.alert_parser import AlertParser
from parsers.vehicle_position_parser import VehiclePositionParser
from utils.transformations import deduplicate_before_parquet

class TransformPbToParquetConfig:
    def __init__(self, config: dict):
        self.input_dir = Path(config['etl']['input_dir'])
        self.output_dir = Path(config['etl']['output_dir'])
        self.max_pb_files_per_folder = config['etl'].get('max_pb_files_per_folder', 50)
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Config initialized: {self.__dict__}")

class TransformPbToParquet:
    def __init__(self, config: TransformPbToParquetConfig):
        self.config = config
        self.observer = Observer()
        self.loop = asyncio.get_event_loop()
        logger.debug("TransformPbToParquet initialized.")

    async def run(self):
        await self.start_monitoring()

    async def start_monitoring(self):
        event_handler = NewFolderHandler(self)
        self.observer.schedule(event_handler, str(self.config.input_dir), recursive=True)
        self.observer.start()
        logger.info(f"Monitoring started: {self.config.input_dir}")

        try:
            while True:
                await self.process_ready_folders()
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            self.observer.stop()
            self.observer.join()
            logger.info("Monitoring stopped.")
        except Exception as e:
            logger.exception(f"Unexpected error during monitoring: {e}")
            self.observer.stop()
            self.observer.join()

    async def process_ready_folders(self):
        for folder in self.config.input_dir.iterdir():
            if folder.is_dir() and (folder / ".done").exists():
                logger.info(f"Found ready folder: {folder}")
                await self.transform_folder(folder)
                self.cleanup_done_file(folder)

    def cleanup_done_file(self, folder: Path):
        done_file = folder / ".done"
        if done_file.exists():
            done_file.unlink()
            logger.info(f"Removed .done file from: {folder}")

    async def transform_folder(self, folder: Path):
        logger.info(f"Processing folder: {folder}")
        pb_files = sorted(folder.glob("*.pb"))[:self.config.max_pb_files_per_folder]
        logger.debug(f"Number of .pb files to process: {len(pb_files)}")

        if not pb_files:
            logger.warning(f"No .pb files found in folder: {folder}")
            return

        all_trip_updates = []
        all_vehicle_positions = []
        all_alerts = []

        for pb_file in pb_files:
            try:
                data = self.pb_to_data(pb_file)
                all_trip_updates.extend(data['trip_updates'])
                all_vehicle_positions.extend(data['vehicle_positions'])
                all_alerts.extend(data['alerts'])
                logger.debug(f"Processed file: {pb_file}")
            except Exception as e:
                logger.exception(f"Error processing file {pb_file}: {e}")

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

        await self.save_dataframe(
            data=all_trip_updates,
            df_name="Trip Updates",
            sub_dir="dynamic/trip_updates",
            timestamp=timestamp,
            columns=['entity_id', 'is_deleted', 'trip_id', 'route_id', 'start_time', 'start_date',
                     'stop_sequence', 'stop_id', 'arrival_delay', 'departure_delay',
                     'schedule_relationship', 'timestamp', 'delay']
        )

        await self.save_dataframe(
            data=all_vehicle_positions,
            df_name="Vehicle Positions",
            sub_dir="dynamic/vehicle_positions",
            timestamp=timestamp,
            columns=['entity_id', 'is_deleted', 'trip_id', 'latitude', 'longitude',
                     'speed', 'bearing', 'occupancy_status', 'timestamp']
        )

        await self.save_dataframe(
            data=all_alerts,
            df_name="Alerts",
            sub_dir="alerts",
            timestamp=timestamp,
            columns=['entity_id', 'is_deleted', 'alert_start', 'alert_end', 'cause',
                     'effect', 'url', 'header_text', 'description_text',
                     'agency_id', 'route_id', 'stop_id', 'trip_id']
        )

        if all_alerts:
            combined_data = all_trip_updates + all_vehicle_positions + all_alerts
            combined_df = pd.DataFrame(combined_data)
            combined_df = deduplicate_before_parquet(combined_df)
            output_dir = self.config.output_dir / "feeds"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"feeds_{timestamp}.parquet"
            combined_df.to_parquet(output_file, index=False)
            logger.info(f"Saved combined Feeds Parquet file: {output_file}")
        else:
            logger.info("No alerts present. Feeds.parquet will not be created.")

    async def save_dataframe(self, data: List[Dict], df_name: str, sub_dir: str,
                            timestamp: str, columns: List[str]):
        if data:
            try:
                df = pd.DataFrame(data, columns=columns)
                # Deduplicate before parquet
                df = deduplicate_before_parquet(df)

                output_dir = self.config.output_dir / sub_dir
                output_dir.mkdir(parents=True, exist_ok=True)

                output_file = output_dir / f"{sub_dir.split('/')[-1]}_{timestamp}.parquet"
                df.to_parquet(output_file, index=False)
                logger.info(f"Saved {df_name} Parquet file: {output_file}")
            except Exception as e:
                logger.error(f"Failed to save {df_name} Parquet file: {e}")
        else:
            logger.warning(f"No {df_name.lower()} data to save from folder.")

    def pb_to_data(self, pb_file: Path) -> Dict[str, List[dict]]:
        with pb_file.open('rb') as f:
            pb_data = f.read()

        feed_message = FeedMessage()
        feed_message.ParseFromString(pb_data)
        logger.debug(f"Parsing file: {pb_file}, entities: {len(feed_message.entity)}")

        trip_updates = []
        vehicle_positions = []
        alerts = []

        for entity in feed_message.entity:
            base_data = {"entity_id": entity.id, "is_deleted": entity.is_deleted}
            if entity.HasField("trip_update"):
                trip_updates.extend(TripUpdateParser.parse(base_data, entity.trip_update))
            if entity.HasField("vehicle"):
                vehicle_positions.append(VehiclePositionParser.parse(base_data, entity.vehicle))
            if entity.HasField("alert"):
                alerts.extend(AlertParser.parse(base_data, entity.alert))

        return {
            "trip_updates": trip_updates,
            "vehicle_positions": vehicle_positions,
            "alerts": alerts
        }

class NewFolderHandler(FileSystemEventHandler):
    def __init__(self, transformer: TransformPbToParquet):
        self.transformer = transformer

    def on_created(self, event):
        if event.is_directory:
            logger.info(f"New folder created: {event.src_path}")
            asyncio.run_coroutine_threadsafe(
                self.transformer.transform_folder(Path(event.src_path)),
                self.transformer.loop
            )

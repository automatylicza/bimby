import time
from pathlib import Path
from typing import Tuple
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from loguru import logger

from .schema_manager import SchemaManager
from .version_manager import DataVersionManager
from .processed_managers import ProcessedFoldersManager, ProcessedFilesManager
from utils.db_utils import remove_existing_keys
from utils.transformations import transform_static_df, transform_dynamic_df

class DataLoader:
    def __init__(self, config: dict):
        self.config = config
        self.engine = create_engine(self.config['database']['uri'])
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        self.schema_manager = SchemaManager(self.engine)
        self.version_manager = DataVersionManager(self.session)
        self.processed_folders = ProcessedFoldersManager(self.session)
        self.processed_files = ProcessedFilesManager(self.session)

        self.static_data_path = Path(self.config['data_storage']['processed_dir'])
        self.dynamic_trip_updates_path = Path(self.config['data_storage']['dynamic_dir']) / 'trip_updates'
        self.dynamic_vehicle_positions_path = Path(self.config['data_storage']['dynamic_dir']) / 'vehicle_positions'
        self.vehicle_dictionary_path = Path(self.config['data_storage'].get('vehicle_dictionary_dir', ''))

        self.check_interval = self.config.get('check_interval', 30)
        self.stop_requested = False

    def run_initial_setup(self):
        self.schema_manager.create_tables_if_not_exists()

    def load_static_data(self):
        new_gtfs_folders = [f for f in sorted(self.static_data_path.glob('gtfs_*')) if not self.processed_folders.is_folder_processed(f.name)]
        new_vehicle_dict = self.vehicle_dictionary_path.exists() and not self.processed_folders.is_folder_processed(self.vehicle_dictionary_path.name)

        if not new_gtfs_folders and not new_vehicle_dict:
            current_version = self.version_manager.get_current_version()
            if current_version:
                logger.info(f"Brak nowych danych statycznych. Używam istniejącej wersji: {current_version}")
            else:
                current_version = self.version_manager.create_new_version("Pierwsza wersja danych statycznych")
            return
        else:
            new_version_id = self.version_manager.create_new_version(description="Nowe dane statyczne")
            for folder in new_gtfs_folders:
                self.load_static_folder(folder, new_version_id)
                self.processed_folders.mark_folder_as_processed(folder.name)

            if new_vehicle_dict:
                # Vehicle dictionary - nie ładujemy do bazy, tylko do Parquet, co już się dzieje gdzie indziej.
                # Oznaczamy folder jako przetworzony, ale nie ładujemy do bazy
                self.processed_folders.mark_folder_as_processed(self.vehicle_dictionary_path.name)

            logger.info(f"Załadowano nowe dane statyczne do wersji {new_version_id}.")

    def load_static_folder(self, folder_path: Path, version_id: int):
        file_mapping = {
            'agency': 'agency.parquet',
            'feed_info': 'feed_info.parquet',
            'stops': 'stops.parquet',
            'routes': 'routes.parquet',
            'calendar': 'calendar.parquet',
            'calendar_dates': 'calendar_dates.parquet',
            'shapes': 'shapes.parquet',
            'trips': 'trips.parquet',
            'stop_times': 'stop_times.parquet'
        }

        for table_name, file_name in file_mapping.items():
            file_path = folder_path / file_name
            if file_path.exists():
                self.load_static_file(file_path, table_name, version_id)

    def load_static_file(self, file_path: Path, table_name: str, version_id: int):
        # Pomijamy vehicle_dictionary - nie ładujemy do bazy
        if table_name == 'vehicle_dictionary':
            logger.info(f"Pomijam ładowanie vehicle_dictionary do bazy. Plik: {file_path}")
            return

        df = pd.read_parquet(file_path)
        df['version_id'] = version_id
        df = transform_static_df(df, table_name)

        # Usuwamy duplikaty przed wstawieniem do bazy
        before_len = len(df)
        df = df.drop_duplicates()
        removed = before_len - len(df)
        if removed > 0:
            logger.info(f"Usunięto {removed} duplikatów przed wstawieniem do tabeli {table_name}.")

        df.to_sql(table_name, self.engine, if_exists='append', index=False)
        logger.info(f"Załadowano {len(df)} rekordów do tabeli {table_name} z pliku {file_path}.")

    def load_dynamic_data(self):
        current_version = self.version_manager.get_current_version()
        if not current_version:
            logger.error("Brak wersji danych statycznych. Najpierw załaduj dane statyczne.")
            return

        self._load_dynamic_files(self.dynamic_trip_updates_path, 'trip_updates', current_version,
                                 pk_cols=('trip_id', 'timestamp', 'version_id'))
        self._load_dynamic_files(self.dynamic_vehicle_positions_path, 'vehicle_positions', current_version,
                                 pk_cols=('entity_id', 'timestamp', 'version_id'))

    def _load_dynamic_files(self, path: Path, table_name: str, version_id: int, pk_cols: Tuple[str, ...]):
        if not path.exists():
            logger.warning(f"Brak folderu {path}")
            return

        files = sorted(path.glob('*.parquet'))
        if not files:
            logger.info(f"Brak plików do załadowania w {path}")
            return

        new_files = [f for f in files if not self.processed_files.is_file_processed(str(f))]
        if not new_files:
            logger.info(f"Brak nowych plików do przetworzenia w {path}")
            return

        # Wczytanie valid_trip_ids do walidacji
        trips_in_db = pd.read_sql(f"SELECT trip_id FROM trips WHERE version_id={version_id}", self.engine)
        valid_trip_ids = set(trips_in_db['trip_id'])

        for file_path in new_files:
            df = pd.read_parquet(file_path)
            df['version_id'] = version_id
            df = transform_dynamic_df(df, table_name, valid_trip_ids=valid_trip_ids)

            if df.empty:
                logger.info(f"Po czyszczeniu brak danych do załadowania z pliku {file_path}.")
                self.processed_files.mark_file_as_processed(str(file_path))
                continue

            with self.engine.connect() as conn:
                df = remove_existing_keys(df, conn, table_name, pk_cols)

            if df.empty:
                logger.info(f"Po usunięciu duplikatów brak danych do załadowania z pliku {file_path}.")
                self.processed_files.mark_file_as_processed(str(file_path))
                continue

            try:
                df.to_sql(table_name, self.engine, if_exists='append', index=False)
                self.processed_files.mark_file_as_processed(str(file_path))
                logger.info(f"Załadowano {len(df)} rekordów do tabeli {table_name} z pliku {file_path}.")
            except Exception as e:
                logger.exception(f"Błąd podczas wstawiania danych z pliku {file_path} do tabeli {table_name}: {e}")

    def run(self):
        """
        Uruchamia cykliczne ładowanie danych statycznych i dynamicznych w pętli.
        """
        self.run_initial_setup()
        logger.info("Uruchamianie cyklicznego ładowania danych statycznych i dynamicznych.")
        while not self.stop_requested:
            try:
                self.load_static_data()
                self.load_dynamic_data()
            except Exception as e:
                logger.exception(f"Błąd podczas cyklicznego przetwarzania danych: {e}")

            if self.stop_requested:
                break

            logger.info(f"Oczekiwanie {self.check_interval} sekund do następnego sprawdzenia.")
            time.sleep(self.check_interval)

    def stop(self):
        self.stop_requested = True
        logger.info("Przerwano cykliczne ładowanie danych.")

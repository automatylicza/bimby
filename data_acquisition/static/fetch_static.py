import asyncio
import logging
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import aiohttp
import pandas as pd
from pydantic import BaseModel

from utils.hash_utils import calculate_hash
from utils.last_modified_manager import LastModifiedManager
from utils.retry import retry_async
from etl.transform_static_to_parquet import TransformStaticToParquet

logger = logging.getLogger(__name__)


class StaticDataFetcherConfig(BaseModel):
    interval_seconds: int
    urls: Dict[str, str]
    raw_dir: Path
    output_dir: Path
    use_hash: bool = False


class StaticDataFetcher:
    def __init__(self, config: dict):
        self.config = StaticDataFetcherConfig(
            interval_seconds=config['data_acquisition']['static']['interval_seconds'],
            urls=config['data_acquisition']['static']['urls'],
            raw_dir=Path(config['data_storage']['raw_dir']) / "static",
            output_dir=Path(config['data_storage']['processed_dir']),
            use_hash=config['data_acquisition']['static'].get('use_hash', False),
    
        )
        self.raw_dir: Path = self.config.raw_dir
        self.output_dir: Path = self.config.output_dir
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.last_modified_manager = LastModifiedManager(self.raw_dir / "metadata")

    def unpack_gtfs_with_timestamp(self, zip_path: Path, timestamp: str) -> None:
        """
        Rozpakowuje plik ZIP, przetwarza na Parquet i zapisuje w katalogu z timestampem.

        Args:
            zip_path (Path): Ścieżka do pliku ZIP.
            timestamp (str): Znacznik czasu używany w nazwie katalogu.
        """
        extract_dir = self.output_dir / f"gtfs_{timestamp}"
        extract_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        logger.info(f"Rozpakowano plik ZIP {zip_path} do {extract_dir}")

        # Przetwarzanie plików GTFS na Parquet
        transformer = TransformStaticToParquet(
            input_dir=extract_dir,
            output_dir=self.output_dir / f"gtfs_{timestamp}",
        )
        transformer.transform()

        # Usuwanie plików .txt po transformacji
        for txt_file in extract_dir.glob("*.txt"):
            txt_file.unlink()
            logger.debug(f"Usunięto plik tekstowy: {txt_file}")

    def process_vehicle_dictionary(self, csv_path: Path, timestamp: str) -> None:
        """
        Przetwarza plik CSV Vehicle Dictionary na Parquet i zapisuje w nowym folderze z timestampem.

        Args:
            csv_path (Path): Ścieżka do pliku CSV.
            timestamp (str): Znacznik czasu używany w nazwie folderu.
        """
        folder = self.output_dir / f"vehicle_dictionary_{timestamp}"
        folder.mkdir(parents=True, exist_ok=True)

        try:
            df = pd.read_csv(csv_path)

            # Zapis do Parquet w odpowiednim folderze
            processed_file = folder / f"vehicle_dictionary_{timestamp}.parquet"
            df.to_parquet(processed_file, index=False)
            logger.info(f"Zapisano plik: {processed_file}")
        except Exception as e:
            logger.exception(f"Błąd podczas przetwarzania pliku {csv_path}: {e}")

    @retry_async(exceptions=(aiohttp.ClientError,), tries=3, delay=2, logger=logger)
    async def fetch_file(
        self,
        session: aiohttp.ClientSession,
        key: str,
        url: str,
        filename: str
    ) -> Optional[Path]:
        """
        Pobiera plik z podanego URL i zapisuje go na dysku, jeśli jest nowszy niż poprzedni.

        Args:
            session (aiohttp.ClientSession): Sesja HTTP do wykonywania żądań.
            key (str): Klucz identyfikujący plik.
            url (str): URL do pobrania pliku.
            filename (str): Nazwa pliku do zapisania.

        Returns:
            Optional[Path]: Ścieżka do zapisanego pliku lub None, jeśli plik nie został zaktualizowany.
        """
        metadata = self.last_modified_manager.get_metadata(key, url)
        existing_hash = metadata.get('Hash')
        logger.debug(f"Zapisany hash dla {url}: {existing_hash}")

        async with session.get(url) as response:
            response.raise_for_status()
            new_data = await response.read()
            new_hash = calculate_hash(new_data)
            logger.debug(f"Nowy hash dla {url}: {new_hash}")

            if new_hash == existing_hash:
                logger.info(f"Brak nowych danych dla {url} (hash nie zmienił się).")
                return None
            else:
                timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                filename_with_timestamp = f"{key}_{timestamp}{Path(filename).suffix}"
                return self._save_file(key, url, filename_with_timestamp, new_data)

    def _save_file(
        self,
        key: str,
        url: str,
        filename: str,
        data: bytes
    ) -> Path:
        """
        Zapisuje dane do pliku i aktualizuje metadane.

        Args:
            key (str): Klucz identyfikujący plik.
            url (str): URL z którego pochodzi plik.
            filename (str): Nazwa pliku do zapisania.
            data (bytes): Dane pliku.

        Returns:
            Path: Ścieżka do zapisanego pliku.
        """
        output_dir = self.raw_dir / key
        output_dir.mkdir(parents=True, exist_ok=True)
        filepath = output_dir / filename
        filepath.write_bytes(data)
        logger.info(f"Pobrano dane statyczne: {filepath}")

        file_hash = calculate_hash(data)
        metadata = {'Hash': file_hash}
        self.last_modified_manager.set_metadata(key, url, metadata)
        return filepath

    async def process_file(
        self,
        session: aiohttp.ClientSession,
        key: str,
        url: str,
        filename: str
    ) -> None:
        """
        Przetwarza pojedynczy plik: pobiera go, rozpakowuje lub zapisuje odpowiednio do rodzaju danych.

        Args:
            session (aiohttp.ClientSession): Sesja HTTP do wykonywania żądań.
            key (str): Klucz identyfikujący plik.
            url (str): URL do pobrania pliku.
            filename (str): Nazwa pliku do zapisania.
        """
        try:
            file_path = await self.fetch_file(session, key, url, filename)
            if file_path:
                timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                if file_path.suffix.lower() == '.zip' and key == 'gtfs_zip':
                    self.unpack_gtfs_with_timestamp(file_path, timestamp)
                elif file_path.suffix.lower() == '.csv' and key == 'vehicle_dictionary':
                    self.process_vehicle_dictionary(file_path, timestamp)
                else:
                    logger.warning(f"Nieznany format pliku lub klucz: {file_path}")
            else:
                logger.info(f"Brak nowych plików do przetworzenia dla klucza: {key}")
        except Exception as e:
            logger.exception(f"Błąd podczas przetwarzania pliku {filename}: {e}")

    async def fetch_all(self) -> None:
        """
        Pobiera wszystkie zdefiniowane dane statyczne.
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for key, url in self.config.urls.items():
                suffix = '.zip' if key == 'gtfs_zip' else '.csv'
                filename = f"{key}{suffix}"
                tasks.append(self.process_file(session, key, url, filename))
            await asyncio.gather(*tasks)

    async def run(self) -> None:
        """
        Uruchamia ciągłe pobieranie danych w zadanym interwale czasowym.
        """
        while True:
            try:
                logger.info("Rozpoczynanie pobierania danych statycznych.")
                await self.fetch_all()
                logger.info("Zakończono pobieranie danych statycznych.")
            except Exception as e:
                logger.exception(f"Błąd podczas pobierania danych statycznych: {e}")
            await asyncio.sleep(self.config.interval_seconds)

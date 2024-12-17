# data_acquisition/dynamic/fetch_dynamic.py

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
from pydantic import BaseModel
from typing import Dict

from utils.retry import retry_async
from utils.folder_manager import FolderManager

logger = logging.getLogger(__name__)

class DynamicDataFetcherConfig(BaseModel):
    interval_seconds: int
    urls: Dict[str, str]
    raw_dir: Path

class DynamicDataFetcher:
    def __init__(self, config):
        self.config = DynamicDataFetcherConfig(
            interval_seconds=config['data_acquisition']['dynamic']['interval_seconds'],
            urls=config['data_acquisition']['dynamic']['urls'],
            raw_dir=Path(config['data_storage']['raw_dir']) / "dynamic"
        )
        self.raw_dir = self.config.raw_dir
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.max_files_per_folder = config['data_acquisition']['dynamic'].get('max_files_per_folder', 10)
        self.folder_manager = FolderManager(self.raw_dir, self.max_files_per_folder)

    @retry_async(exceptions=(aiohttp.ClientError,), tries=3, delay=2, logger=logger)
    async def fetch(self, session: aiohttp.ClientSession, key: str, url: str):
        """
        Asynchronicznie pobiera dane z podanego URL i zapisuje je do pliku.
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        filename = f"{key}_{timestamp}.pb"
        category_folder = self.folder_manager.get_current_folder(key)
        filepath = category_folder / filename

        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.read()
            filepath.write_bytes(data)
            logger.info(f"Pobrano dane dynamiczne: {filepath}")

    async def fetch_all(self):
        """
        Pobiera wszystkie zdefiniowane dane dynamiczne jednocześnie.
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for key, url in self.config.urls.items():
                tasks.append(self.fetch(session, key, url))
            await asyncio.gather(*tasks, return_exceptions=True)

    async def run(self):
        """
        Uruchamia ciągłe pobieranie danych w zadanym interwale czasowym.
        """
        interval = self.config.interval_seconds
        next_run = datetime.utcnow()
        while True:
            try:
                await self.fetch_all()
            except Exception as e:
                logger.exception(f"Błąd podczas pobierania danych dynamicznych: {e}")
            next_run += timedelta(seconds=interval)
            sleep_duration = (next_run - datetime.utcnow()).total_seconds()
            if sleep_duration > 0:
                await asyncio.sleep(sleep_duration)
            else:
                logger.warning("Pobieranie trwało dłużej niż interwał czasowy.")
                next_run = datetime.utcnow()

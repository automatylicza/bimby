# utils/last_modified_manager.py

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class LastModifiedManager:
    def __init__(self, storage_dir: Path):
        """
        Inicjalizuje menedżera Last-Modified oraz ETag.
        """
        self.storage_dir = storage_dir
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def _get_file_path(self, key: str) -> Path:
        """
        Zwraca ścieżkę do pliku JSON dla danego klucza.
        Tworzy odpowiedni podkatalog, jeśli nie istnieje.
        """
        sub_dir = self.storage_dir / key  # Podkatalog dla danego klucza
        sub_dir.mkdir(parents=True, exist_ok=True)  # Tworzenie podkatalogu
        return sub_dir / "metadata.json"
        
    def load(self, key: str) -> dict:
        """
        Ładuje dane o ostatnich modyfikacjach z pliku JSON.
        Jeśli plik nie istnieje, zwraca pusty słownik.
        """
        file_path = self._get_file_path(key)
        if file_path.exists():
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    logger.debug(f"Dane załadowane z pliku: {file_path} ({len(data)} wpisów)")
                    return data
            except json.JSONDecodeError:
                logger.error(f"Plik {file_path} jest uszkodzony. Zwracam pusty słownik.")
        else:
            logger.debug(f"Plik {file_path} nie istnieje. Tworzę nowy plik.")
        return {}
    
    def save(self, key: str, data: dict):
        """
        Zapisuje dane o ostatnich modyfikacjach do pliku JSON.
        """
        file_path = self._get_file_path(key)
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f)
            logger.debug(f"Dane zapisane w pliku: {file_path} ({len(data)} wpisów)")
        except Exception as e:
            logger.error(f"Nie udało się zapisać pliku {file_path}: {e}")
    
    def get_metadata(self, key: str, url: str) -> dict:
        """
        Pobiera metadane dla danego URL.
        """
        data = self.load(key)
        return data.get(url, {})
    
    def set_metadata(self, key: str, url: str, metadata: dict):
        """
        Ustawia metadane dla danego URL.
        """
        logger.debug(f"Ustawienie metadanych: key={key}, url={url}, metadata={metadata}")
        data = self.load(key)
        data[url] = metadata
        self.save(key, data)

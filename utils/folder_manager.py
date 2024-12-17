# utils/folder_manager.py

from pathlib import Path
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class FolderManager:
    def __init__(self, base_dir: Path, max_files: int = 10):
        self.base_dir = base_dir
        self.max_files = max_files

    def get_current_folder(self, category: str) -> Path:
        """
        Zwraca ścieżkę do aktualnego folderu dla danej kategorii.
        """
        category_dir = self.base_dir / category
        category_dir.mkdir(parents=True, exist_ok=True)

        # Pobierz listę istniejących podfolderów
        subfolders = sorted([f for f in category_dir.iterdir() if f.is_dir()])
        if not subfolders:
            return self._create_new_folder(category_dir)

        # Sprawdź ostatni folder
        last_folder = subfolders[-1]
        num_files = len([f for f in last_folder.iterdir() if f.is_file()])
        if num_files >= self.max_files:
            self._mark_folder_as_done(last_folder)
            return self._create_new_folder(category_dir)
        else:
            return last_folder

    def _create_new_folder(self, category_dir: Path) -> Path:
        new_folder = category_dir / self._create_new_folder_name()
        new_folder.mkdir()
        return new_folder

    def _create_new_folder_name(self) -> str:
        return datetime.utcnow().strftime("%Y%m%d%H%M%S")

    def _mark_folder_as_done(self, folder: Path):
        """
        Tworzy plik `.done` w folderze, aby oznaczyć go jako gotowy do przetwarzania.
        """
        done_file = folder / ".done"
        done_file.touch()
        logger.info(f"Folder marked as done: {folder}")
from sqlalchemy.orm import Session
from sqlalchemy import text
from loguru import logger

class ProcessedFoldersManager:
    def __init__(self, session: Session):
        self.session = session

    def is_folder_processed(self, folder_name: str) -> bool:
        query = text("SELECT 1 FROM processed_folders WHERE folder_name = :folder_name;")
        result = self.session.execute(query, {'folder_name': folder_name})
        return result.fetchone() is not None

    def mark_folder_as_processed(self, folder_name: str):
        query = text("INSERT INTO processed_folders (folder_name) VALUES (:folder_name) ON CONFLICT (folder_name) DO NOTHING;")
        self.session.execute(query, {'folder_name': folder_name})
        self.session.commit()
        logger.info(f"Folder '{folder_name}' oznaczony jako przetworzony.")


class ProcessedFilesManager:
    def __init__(self, session: Session):
        self.session = session

    def is_file_processed(self, file_path: str) -> bool:
        query = text("SELECT 1 FROM processed_files WHERE file_path = :file_path;")
        result = self.session.execute(query, {'file_path': file_path})
        return result.fetchone() is not None

    def mark_file_as_processed(self, file_path: str):
        query = text("INSERT INTO processed_files (file_path) VALUES (:file_path) ON CONFLICT (file_path) DO NOTHING;")
        self.session.execute(query, {'file_path': file_path})
        self.session.commit()
        logger.info(f"Plik '{file_path}' oznaczony jako przetworzony.")

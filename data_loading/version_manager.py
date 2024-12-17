from sqlalchemy.orm import Session
from sqlalchemy import text
from loguru import logger

class DataVersionManager:
    def __init__(self, session: Session):
        self.session = session

    def create_new_version(self, description: str = '') -> int:
        query = text("INSERT INTO static_data_versions (description) VALUES (:description) RETURNING version_id;")
        result = self.session.execute(query, {'description': description})
        self.session.commit()
        version_id = result.fetchone()[0]
        logger.info(f"Utworzono nową wersję danych statycznych: {version_id}")
        return version_id

    def get_current_version(self) -> int:
        query = text("SELECT version_id FROM static_data_versions ORDER BY version_id DESC LIMIT 1;")
        result = self.session.execute(query)
        row = result.fetchone()
        if row:
            return row[0]
        return None

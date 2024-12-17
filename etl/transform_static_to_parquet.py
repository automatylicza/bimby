import pandas as pd
from pathlib import Path
from loguru import logger

class TransformStaticToParquet:
    def __init__(self, input_dir: Path, output_dir: Path):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def transform(self):
        gtfs_files = ['agency.txt', 'stops.txt', 'routes.txt', 'trips.txt', 'stop_times.txt',
                      'calendar.txt', 'calendar_dates.txt', 'shapes.txt', 'feed_info.txt']

        for gtfs_file in gtfs_files:
            input_file = self.input_dir / gtfs_file
            if input_file.exists():
                try:
                    df = pd.read_csv(input_file)
                    output_file = self.output_dir / f"{gtfs_file.replace('.txt', '')}.parquet"
                    df.to_parquet(output_file, index=False)
                    logger.info(f"Przetworzono plik: {input_file} -> {output_file}")
                except Exception as e:
                    logger.exception(f"Błąd podczas przetwarzania pliku {input_file}: {e}")
            else:
                logger.warning(f"Plik {input_file} nie istnieje.")

        # Przetwarzanie vehicle_dictionary.csv
        vehicle_dict_file = self.input_dir / 'vehicle_dictionary.csv'
        if vehicle_dict_file.exists():
            try:
                df = pd.read_csv(vehicle_dict_file)
                output_file = self.output_dir / "vehicle_dictionary.parquet"
                df.to_parquet(output_file, index=False)
                logger.info(f"Przetworzono plik: {vehicle_dict_file} -> {output_file}")
            except Exception as e:
                logger.exception(f"Błąd podczas przetwarzania pliku {vehicle_dict_file}: {e}")
        else:
            logger.warning(f"Plik {vehicle_dict_file} nie istnieje.")


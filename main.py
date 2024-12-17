import asyncio
import argparse
from loguru import logger
from utils.logging_config import configure_logging
from data_acquisition.dynamic.fetch_dynamic import DynamicDataFetcher
from data_acquisition.static.fetch_static import StaticDataFetcher
from etl.transform_pb_to_parquet import TransformPbToParquet, TransformPbToParquetConfig
from data_loading.load_to_db import DataLoader
from config import CONFIG

async def main_async(config, modules_to_run):
    modules_config = config.get('modules', {})
    tasks = []
    data_loader = None

    if modules_to_run:
        for module in modules_config.keys():
            modules_config[module] = module in modules_to_run

    if modules_config.get('fetch_dynamic', False):
        dynamic_fetcher = DynamicDataFetcher(config)
        tasks.append(asyncio.create_task(dynamic_fetcher.run()))
        logger.info("Moduł 'fetch_dynamic' został uruchomiony.")

    if modules_config.get('fetch_static', False):
        static_fetcher = StaticDataFetcher(config)
        tasks.append(asyncio.create_task(static_fetcher.run()))
        logger.info("Moduł 'fetch_static' został uruchomiony.")

    if modules_config.get('etl', False):
        etl_config = TransformPbToParquetConfig(config)
        etl_module = TransformPbToParquet(etl_config)
        tasks.append(asyncio.create_task(etl_module.run()))
        logger.info("Moduł 'etl' został uruchomiony.")

    if modules_config.get('load_to_db', False):
        data_loader = DataLoader(config)
        tasks.append(asyncio.create_task(run_data_loader(data_loader)))
        logger.info("Moduł 'load_to_db' został uruchomiony.")

    if not tasks:
        logger.warning("Żaden moduł nie jest aktywny. Sprawdź konfigurację.")
        return

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Zatrzymano aplikację przez użytkownika (KeyboardInterrupt).")
        if data_loader:
            data_loader.stop()
    except Exception as e:
        logger.exception(f"Wystąpił nieoczekiwany błąd: {e}")
        if data_loader:
            data_loader.stop()
    finally:
        logger.info("Zakończono działanie main_async.")

async def run_data_loader(data_loader: DataLoader):
    await asyncio.to_thread(data_loader.run)

def main():
    parser = argparse.ArgumentParser(description="Projekt Bimba - Pobieranie Danych")
    parser.add_argument('--config', type=str, default='config/config.yaml', help='Ścieżka do pliku konfiguracyjnego')
    parser.add_argument('--modules', nargs='*', help='Lista modułów do uruchomienia (opcjonalnie)')
    args = parser.parse_args()

    config = CONFIG
    log_file = config['logging']['file']
    log_level = config['logging']['level']
    log_rotation = config['logging'].get('rotation', "10 MB")
    log_compression = config['logging'].get('compression', "zip")
    configure_logging(log_file, log_level, rotation=log_rotation, compression=log_compression)

    logger.info("Aplikacja rozpoczęła działanie.")

    try:
        asyncio.run(main_async(config, args.modules))
    except KeyboardInterrupt:
        logger.info("Zatrzymano aplikację przez użytkownika.")
    except Exception as e:
        logger.exception(f"Wystąpił nieoczekiwany błąd: {e}")
    finally:
        logger.info("Aplikacja została zakończona.")

if __name__ == "__main__":
    main()

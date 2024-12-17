import pandas as pd
from loguru import logger

def transform_static_df(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Wykonuje transformacje i rzutowania dla danych statycznych.
    Nie usuwa kolumn, tylko dokonuje konwersji dat.

    :param df: DataFrame z danymi statycznymi
    :param table_name: nazwa tabeli np. 'calendar', 'feed_info', 'calendar_dates'
    :return: Przetransformowany DataFrame
    """
    if table_name == 'calendar':
        df['start_date'] = pd.to_datetime(df['start_date'], format='%Y%m%d', errors='coerce').dt.date
        df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d', errors='coerce').dt.date
    elif table_name == 'feed_info':
        df['feed_start_date'] = pd.to_datetime(df['feed_start_date'], format='%Y%m%d', errors='coerce').dt.date
        df['feed_end_date'] = pd.to_datetime(df['feed_end_date'], format='%Y%m%d', errors='coerce').dt.date
    elif table_name == 'calendar_dates':
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce').dt.date
    return df

def transform_dynamic_df(df: pd.DataFrame, table_name: str, valid_trip_ids: set = None) -> pd.DataFrame:
    """
    Transformacje dla danych dynamicznych:
    - usunięcie duplikatów
    - zastąpienie '' w stop_id na None
    - weryfikacja trip_id
    """
    # Usuwanie duplikatów
    before_len = len(df)
    if table_name == 'trip_updates':
        pk_cols = ['trip_id', 'timestamp', 'version_id']
    else:
        pk_cols = ['entity_id', 'timestamp', 'version_id']

    df.drop_duplicates(subset=pk_cols, inplace=True)
    after_len = len(df)
    removed = before_len - after_len
    if removed > 0:
        logger.info(f"Z pliku usunięto {removed} duplikatów (tabela {table_name}).")

    if 'stop_id' in df.columns:
        df['stop_id'] = df['stop_id'].apply(lambda x: None if x == '' else x)

    # Walidacja trip_id
    if valid_trip_ids is not None and 'trip_id' in df.columns and table_name in ['trip_updates', 'vehicle_positions']:
        before_count = len(df)
        if table_name == 'trip_updates':
            df = df[df['trip_id'].isin(valid_trip_ids)]
        elif table_name == 'vehicle_positions':
            mask_trip_not_null = df['trip_id'].notna()
            valid_mask = ~mask_trip_not_null | (df['trip_id'].isin(valid_trip_ids))
            df = df[valid_mask]
        after_count = len(df)
        if after_count < before_count:
            logger.warning(f"Usunięto {before_count - after_count} rekordów z nieistniejącym trip_id w {table_name}.")

    return df

def deduplicate_before_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Usuwa duplikaty przed zapisem do Parquet.
    """
    before_len = len(df)
    df = df.drop_duplicates()
    removed = before_len - len(df)
    if removed > 0:
        logger.info(f"Usunięto {removed} duplikatów przed zapisem do Parquet.")
    return df

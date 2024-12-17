import pandas as pd
from sqlalchemy import text
from loguru import logger

def remove_existing_keys(df: pd.DataFrame, conn, table_name: str, pk_cols: tuple) -> pd.DataFrame:
    """
    Usuwa z df rekordy, które już istnieją w bazie (na podstawie kluczy głównych).
    """
    if df.empty:
        return df

    keys = list(df[list(pk_cols)].itertuples(index=False, name=None))
    if not keys:
        return df

    try:
        with conn.begin():
            temp_table = "temp_keys"
            create_temp = f"""
                CREATE TEMPORARY TABLE {temp_table} (
                    {pk_cols[0]} TEXT,
                    {pk_cols[1]} BIGINT,
                    {pk_cols[2]} INT
                ) ON COMMIT DROP;
            """
            conn.execute(text(create_temp))
            logger.debug(f"Tymczasowa tabela {temp_table} została utworzona.")

            temp_df = pd.DataFrame(keys, columns=pk_cols)
            temp_df.to_sql(temp_table, conn, if_exists='append', index=False)
            logger.debug(f"Wstawiono {len(temp_df)} kluczy do tymczasowej tabeli {temp_table}.")

            select_query = f"""
                SELECT t.{pk_cols[0]}, t.{pk_cols[1]}, t.{pk_cols[2]}
                FROM {table_name} t
                INNER JOIN {temp_table} tk
                ON t.{pk_cols[0]} = tk.{pk_cols[0]}
                AND t.{pk_cols[1]} = tk.{pk_cols[1]}
                AND t.{pk_cols[2]} = tk.{pk_cols[2]}
            """
            existing = conn.execute(text(select_query)).fetchall()
            existing_set = set(existing)
            logger.debug(f"Znaleziono {len(existing_set)} istniejących kluczy w tabeli {table_name}.")

    except Exception as e:
        logger.exception(f"Błąd podczas wykonywania zapytania: {e}")
        return df

    if existing_set:
        before_len = len(df)
        df = df[~df.apply(lambda r: (r[pk_cols[0]], r[pk_cols[1]], r[pk_cols[2]]) in existing_set, axis=1)]
        after_len = len(df)
        removed = before_len - after_len
        if removed > 0:
            logger.info(f"Usunięto {removed} rekordów z powodu istniejących kluczy w {table_name}.")

    return df

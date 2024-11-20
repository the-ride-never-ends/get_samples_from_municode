

import pandas as pd


from database.MySqlDatabase import MySqlDatabase


async def get_gnis_df_and_url_hash_df_from_mysql_database(db: MySqlDatabase) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch GNIS data and processed URL hashes from the MySQL database.

    Args:
        db (MySqlDatabase): The database connection object.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing two DataFrames:
            - gnis_df: DataFrame with distinct GNIS values.
            - url_hash_df: DataFrame with distinct URL hashes and their associated GNIS.
    """
    gnis_df = await db.async_query_to_dataframe("""
    SELECT DISTINCT gnis FROM sources WHERE source_municode IS NOT NULL;
    """)

    url_hash_df: pd.DataFrame = await db.async_query_to_dataframe("""
        SELECT DISTINCT d.url_hash, d.gnis 
        FROM doc_content d
            JOIN sources s ON d.gnis = s.gnis
            WHERE d.url_hash IS NOT NULL 
            AND s.source_municode IS NOT NULL;
    """)
    return gnis_df, url_hash_df

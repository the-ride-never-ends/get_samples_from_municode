import pandas as pd
from tqdm import tqdm


from database.MySqlDatabase import MySqlDatabase
from logger.logger import Logger
logger = Logger(logger_name=__name__)


async def load_municode_urls_from_mysql_database(
                                        db: MySqlDatabase,
                                        gnis_df: pd.DataFrame,
                                        urls_df_list: list[pd.DataFrame]
                                        ) -> list[pd.DataFrame]:
    """
    Load Municode URLs from MySQL database for GNIS entries not found in CSV files.

    This function queries the MySQL database for URLs associated with GNIS entries
    that were not found in the CSV files. It appends the results to the existing
    urls_df_list and sorts the list by municode_url_depth.

    Args:
        db (MySqlDatabase): An instance of the MySqlDatabase class for database operations.
        gnis_df (pd.DataFrame): A DataFrame containing GNIS entries to query.
        urls_df_list (list[pd.DataFrame]): A list of DataFrames containing previously loaded URLs.

    Returns:
        list[pd.DataFrame]: An updated and sorted list of DataFrames containing URLs.

    Note:
        - The function queries URLs that are not already present in the doc_content table.
        - It limits the query results to 10000 URLs per GNIS entry.
        - The returned list is sorted by the 'municode_url_depth' column.
    """

    if len(gnis_df) > 0: # If there's any left over, try to get them from the database.
        for row in tqdm(gnis_df.itertuples(index=False, name=None), desc="Loading remaining URLs from database"):
            args = {"gnis": row.gnis}
            db_urls_df = await db.async_query_to_dataframe("""
            SELECT u.url_hash, u.query_hash, u.gnis, u.url, u.municode_url_depth 
            FROM urls u 
            WHERE ( 
                u.gnis = {gnis} 
                AND u.municode_url_depth IS NOT NULL 
                AND u.url_hash NOT IN (SELECT DISTINCT url_hash FROM doc_content) 
            ) 
            LIMIT 10000;
            """, args=args)
            if len(db_urls_df) > 0:
                urls_df_list.append(db_urls_df)
                logger.debug(f"Found {len(db_urls_df)} URLs for GNIS {row.gnis}")
            else:
                logger.warning(f"No URLs found for GNIS {row.gnis}")
    else:
        pass

    # Sort urls_list by municode_url_depth
    urls_df_list.sort(key=lambda x: x['municode_url_depth'])

    logger.info(f"Total cities in urls_df_list: {len(urls_df_list)}")
    total_urls = sum(len(df) for df in urls_df_list)
    logger.debug(f"Total URLs in urls_df_list: {total_urls}")

    return urls_df_list
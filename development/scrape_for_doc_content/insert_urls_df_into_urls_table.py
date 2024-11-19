import pandas as pd


from database.MySqlDatabase import MySqlDatabase
from database.utils.database.get_num_placeholders import get_num_placeholders
from database.utils.database.get_column_names import get_column_names
from database.utils.database.get_columns_to_update import get_columns_to_update

from logger.logger import Logger
logger = Logger(logger_name=__name__)


async def insert_urls_df_into_urls_table(output_folder: str, db: MySqlDatabase) -> None:
    """
    Insert URLs from a CSV file into the 'urls' table in the database.

    This function reads a CSV file containing URL data, and inserts or updates the records
    in the 'urls' table of the specified database. If a record already exists (based on the primary key),
    it updates the 'municode_url_depth' column.

    Args:
        output_folder (str): Path to the CSV file containing the URL data.
        db (MySqlDatabase): An instance of MySqlDatabase for database operations.

    Returns:
        None

    Raises:
        Any exceptions raised by pandas.read_csv() or db.async_execute_sql_command()
    """
    urls_df = pd.read_csv(output_folder)
    urls_list = list(urls_df.itertuples(index=False, name=None))
    logger.debug(f"urls_list[0]: {urls_list[0]}",t=30)

    table = "urls"
    column_names = urls_df.columns.tolist()
    update_columns = ['municode_url_depth']

    args = {
        "table": table,
        "placeholders": get_num_placeholders(len(column_names)),
        "columns": get_column_names(column_names),
        "update": get_columns_to_update(update_columns)
    }
    logger.debug(f"args: {args}",t=30)

    await db.async_execute_sql_command("""
    INSERT INTO {table} ({columns}) 
    VALUES ({placeholders}) 
    ON DUPLICATE KEY UPDATE {update};
    """,args=args)
    return

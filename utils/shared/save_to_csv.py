import csv

from logger.logger import Logger

logger = Logger(logger_name=__name__)


def save_to_csv(data: list[dict], filepath: str) -> None:
    """
    Save a list of dictionaries to a CSV file.
    """
    # Check if the list is empty.
    if not data:
        logger.warning("No data to save: list is empty.")
        return

    # Check if all the dictionaries in the list are empty.
    dic_list = [dic for dic in data if 0 < len(dic)]
    if not dic_list:
        logger.warning("No data to save: dictionaries are empty.")
        return
    
    # Add '.csv' extension if not present
    if not filepath.lower().endswith('.csv'):
        filepath += '.csv'

    # Get the csv header's from the first dictionary's keys.
    keys = data[0].keys()

    try:
        with open(filepath, 'w', newline='') as file:
            dict_writer = csv.DictWriter(file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)
    
        logger.info(f"Data saved to {filepath}")
    except IOError as e:
        logger.error(f"Error saving data to {filepath}: {str(e)}")

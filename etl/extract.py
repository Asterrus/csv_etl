import logging

import pandas as pd

from etl.exceptions import CSVReadError

logger = logging.getLogger(__name__)


def read_csv(file_path):
    logger.debug(f"Reading CSV file: {file_path}")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully read CSV file: {file_path}")
    except Exception as e:
        logger.error(f"Error reading CSV file: {file_path}")
        raise CSVReadError(f"Error reading CSV file: {e}")
    return df

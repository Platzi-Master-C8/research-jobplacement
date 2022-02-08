# Python
import os

# Pandas
import pandas as pd
from pandas import DataFrame

# Logger
import logging

ROOT_DIR = os.path.dirname(os.path.dirname((os.path.dirname(os.path.abspath(__file__)))))

# Dir where the data will be stored
DIR_RAW_DATA = os.path.abspath(f'{ROOT_DIR}/data/raw')
DIR_PROCESSED_DATA = os.path.abspath(f'{ROOT_DIR}/data/processed')
DIR_INTERIM_DATA = os.path.abspath(f'{ROOT_DIR}/data/interim')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_data(file_name: str, df_type: str) -> DataFrame:
    """
    Reads data from a csv file and returns a pandas dataframe

    :param file_name: Name of the file to read
    :param df_type: Type of dataframe to return
    :return: Pandas dataframe
    """
    path = ''
    if df_type == 'raw':
        path = os.path.abspath(f'{DIR_RAW_DATA}/{file_name}')

    elif df_type == 'interim':
        path = os.path.abspath(f'{DIR_INTERIM_DATA}/{file_name}')

    elif df_type == 'processed':
        path = os.path.abspath(f'{DIR_PROCESSED_DATA}/{file_name}')

    logger.info(f'Reading {df_type} data from {path}')
    return pd.read_csv(path)


def save_data(df: DataFrame, file_name: str, df_type: str) -> None:
    """
    Saves a pandas dataframe to a csv file in the specified directory

    :param df: Pandas dataframe to save
    :param file_name: Name of the file to save
    :param df_type: Type of dataframe to save
    :return: None
    """
    path = ''
    if df_type == 'raw':
        path = os.path.abspath(f'{DIR_RAW_DATA}/{file_name}')

    elif df_type == 'interim':
        path = os.path.abspath(f'{DIR_INTERIM_DATA}/{file_name}')

    elif df_type == 'processed':
        path = os.path.abspath(f'{DIR_PROCESSED_DATA}/{file_name}')

    logger.info(f'Saving {df_type} data to {path}')
    df.to_csv(path, index=False)

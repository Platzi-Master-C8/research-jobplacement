# Python
import os

# Logger
import logging

# Pandas
import pandas as pd
from pandas import DataFrame

# Scrappers
from data.job_offers import (
    scraping_get_on_board,
    clean_job_offers
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Dir where the data will be stored
DIR_RAW_DATA = os.path.abspath(f'{ROOT_DIR}/data/raw')
DIR_PROCESSED_DATA = os.path.abspath(f'{ROOT_DIR}/data/processed')
DIR_INTERIM_DATA = os.path.abspath(f'{ROOT_DIR}/data/interim')
# List of files to be processed in the pipeline
dirty_job_offers_files_names = []
dirty_salaries_files_names = []
dirty_reviews_files_names = []


def main():
    _extract()
    _transform()
    _load()


def _extract():
    """
    Extract data from the web and save it in the raw data directory

    Loop through the job offers pages and extract the data from each page and save it in the raw data directory
    """
    logger.info('Starting the extraction process')
    data_job_offers = [
        {
            'name': 'Get on board',
            'file_name': 'get_on_board',
        },
        {
            'name': 'Remote OK',
            'file_name': 'remote_ok',
        },
        {
            'name': 'We Work Remotely',
            'file_name': 'we_work_remotely',
        }
    ]
    data_salaries = [
        {
            'name': 'Levels.fyi',
            'file_name': 'levels_fyi',
        }
    ]
    data_reviews = [
        {
            'name': 'Comparably',
            'file_name': 'comparably',
        },
        {
            'name': 'OCC',
            'file_name': 'occ',
        }
    ]

    # Scraping job offers from the data_job_offers list
    for data_source in data_job_offers:
        logger.info(f'Getting job offers from {data_source.get("name")}')
        df_dirty = pd.DataFrame()
        if data_source.get("name") == 'Get on board':
            df_dirty = scraping_get_on_board()
        df_dirty.to_csv(os.path.abspath(f'{DIR_RAW_DATA}/{data_source.get("file_name")}.csv'), index=False)
        dirty_job_offers_files_names.append(f'{data_source.get("file_name")}.csv')
        logger.info(f'Data from {data_source.get("name")} saved in data/raw')

    # Scraping salaries from the data_salaries list
    for data_source in data_salaries:
        logger.info(f'Getting salaries from {data_source.get("name")}')
        pass

    # Scraping reviews from the data_reviews list
    for data_source in data_reviews:
        logger.info(f'Getting reviews from {data_source.get("name")}')
        pass


def _transform():
    """
    Transform the data from the raw data directory to the processed data directory
    """
    logger.info('Starting the transform process')

    for dirty_job_offers_file_name in dirty_job_offers_files_names:
        df_dirty = _read_data(os.path.abspath(f'{DIR_RAW_DATA}/{dirty_job_offers_file_name}'))
        df_clean = clean_job_offers(df_dirty)
        _save_data(df_clean, dirty_job_offers_file_name)

    for dirty_salaries_file_name in dirty_salaries_files_names:
        df_dirty = _read_data(os.path.abspath(f'{DIR_RAW_DATA}/{dirty_salaries_file_name}'))
        df_clean = clean_salaries(df_dirty)
        _save_data(df_clean, dirty_salaries_file_name)

    for dirty_reviews_file_name in dirty_reviews_files_names:
        df_dirty = _read_data(os.path.abspath(f'{DIR_RAW_DATA}/{dirty_reviews_file_name}'))
        df_clean = clean_reviews(df_dirty)
        _save_data(df_clean, dirty_reviews_file_name)


def _load():
    """
    Load the data from the processed data directory to the database or any other storage medium (e.g. CSV file)
    """
    logger.info('Starting the loading process')


def _read_data(data_path: str) -> DataFrame:
    """
    Read the data from the data_path

    :param data_path: the path to the data to be read from the file system
    :return: DataFrame with the data from the data_path file
    """
    logger.info(f'Reading data from {data_path}')
    return pd.read_csv(data_path)


def _save_data(df: DataFrame, filename: str) -> None:
    """
    Save the data from the DataFrame to the data directory

    :param df: DataFrame with the data to be saved
    :param filename: the name of the file to be saved
    :return: None
    """
    clean_filename = f'clean_{filename}'
    logger.info(f'Saving data at location: {clean_filename}')
    df.to_csv(clean_filename, index=False)


if __name__ == '__main__':
    main()

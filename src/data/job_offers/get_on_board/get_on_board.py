# Python
import json

# Request
import requests

# Pandas
import pandas as pd
from pandas import DataFrame

# Utils
from utils.log_generator import generate_log_line
from utils.transform import (
    remove_emojis,
    extract_text_from_html,
    generate_column_uid
)

GET_ON_BOARD_URL = "https://www.getonbrd.com/api/v0/categories/programming/jobs?per_page=100&page="


def request_job_offers(page_number: int) -> list or None:
    """
    This function is used to request the job offers from the API.

    :param page_number: The page number of the job offers.
    :return: JSON data of the job offers.
    """
    url = f'{GET_ON_BOARD_URL}{page_number}&expand=["company", "modality", "seniority"]'
    payload = {'content-type': 'application/json'}

    response = requests.request("GET", url, data=payload)
    generate_log_line(response)
    content = json.loads(response.text)

    if len(content['data']) == 0:
        return None
    return content['data']


def scraping_get_on_board() -> DataFrame:
    """
    Search for the job offers by page number in the API and save them in a dataframe.
    """
    df = pd.DataFrame()
    for page in range(1, 10):
        new_data = request_job_offers(page)
        if new_data is not None:
            df_temp = pd.json_normalize(new_data, max_level=None)
            df = pd.concat([df, df_temp])
        else:
            break
    return df


def clean_job_offers(df: DataFrame) -> DataFrame:
    """
    This function is used to clean the job offers dataframe.

    :param df: The dataframe of the job offers.
    :return: The cleaned dataframe.
    """
    df['attributes.published_at'] = pd.to_datetime(df['attributes.published_at'], unit='s')
    columns_to_rename = {
        'attributes.title': 'position',
        'attributes.company.data.attributes.name': 'company_name',
        'attributes.category_name': 'category_name',
        'attributes.modality.data.attributes.name': 'modality',
        'attributes.projects': 'projects',
        'attributes.description': 'description',
        'attributes.functions': 'functions',
        'attributes.remote': 'remote',
        'attributes.remote_modality': 'remote_modality',
        'attributes.country': 'country',
        'attributes.min_salary': 'salary_min',
        'attributes.max_salary': 'salary_max',
        'attributes.seniority.data.attributes.name': 'seniority',
        'attributes.published_at': 'date_position',
        'links.public_url': 'url_data',
    }

    columns_job = list(columns_to_rename.keys())
    columns_to_clean = ['attributes.description', 'attributes.projects', 'attributes.functions']
    df_data = df.loc[:, columns_job]
    # Extract text from HTML
    df_data.loc[:, columns_to_clean] = df_data.loc[:, columns_to_clean].applymap(extract_text_from_html)
    # Rename the columns names with the new names
    df_data.rename(columns=columns_to_rename, inplace=True)
    df_data.reset_index(inplace=True, drop=True)
    df_data = df_data.convert_dtypes()
    # Set salary_type to USD
    df_data['salary_type'] = 'USD'
    df_data['activate'] = True

    # Clean the projects, functions and description columns to remove emojis
    df_data[['projects', 'description', 'functions']] = df_data[[
        'projects',
        'description',
        'functions'
    ]].applymap(remove_emojis)
    # Add a column with the unique id of the job offer
    df_data = generate_column_uid(df_data)
    return df_data

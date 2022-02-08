# Python
import json
from datetime import datetime

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
from utils.interface import PipelineInterface
from utils.files import (save_data, read_data)


class GetOnBoardPipeline(PipelineInterface):

    def __init__(self):
        self.get_on_board_url = "https://www.getonbrd.com/api/v0/categories/programming/jobs?per_page=100&page="
        self.now = datetime.now()
        self.file_name = f'{self.now.strftime("%Y-%m-%d")}_get_on_board.csv'

    def execute(self):
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        df = pd.DataFrame()
        for page in range(1, 10):
            new_data = self._request_job_offers(page)
            if new_data is not None:
                df_temp = pd.json_normalize(new_data, max_level=None)
                df = pd.concat([df, df_temp])
            else:
                break
        return save_data(df, self.file_name, 'raw')

    def transform(self):
        df_raw = read_data(self.file_name, 'raw')
        df_cleaned = self._clean_job_offers(df_raw)
        return save_data(df_cleaned, self.file_name, 'interim')

    def load(self):
        df_interim = read_data(self.file_name, 'interim')
        # upload data to database
        return save_data(df_interim, self.file_name, 'processed')

    def _request_job_offers(self, page_number: int) -> list or None:
        """
        This method is used to request the job offers from the API.

        :param page_number: The page number of the job offers.
        :return: JSON data of the job offers.
        """
        url = f'{self.get_on_board_url}{page_number}&expand=["company", "modality", "seniority"]'
        payload = {'content-type': 'application/json'}

        response = requests.request("GET", url, data=payload)
        generate_log_line(response)
        content = json.loads(response.text)

        if len(content['data']) == 0:
            return None
        return content['data']

    def _clean_job_offers(self, df: DataFrame) -> DataFrame:
        """
        This method is used to clean the job offers dataframe.

        :param df: The dataframe of the job offers.
        :return: The cleaned dataframe.
        """
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
            'attributes.perks': 'perks',
            'links.public_url': 'url_data',

        }

        # Select the columns to keep and rename them to the new names specified in the dictionary above.
        columns_job = list(columns_to_rename.keys())
        columns_to_clean = ['attributes.description', 'attributes.projects', 'attributes.functions']
        df['attributes.published_at'] = pd.to_datetime(df['attributes.published_at'], unit='s')
        df_data = df.loc[:, columns_job]

        # Fill the missing functions with empty the string to avoid errors in the next step
        df_data['attributes.functions'].fillna('No functions', inplace=True)

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

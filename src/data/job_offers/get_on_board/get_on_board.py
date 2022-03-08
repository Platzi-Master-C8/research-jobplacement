# Python
import json
from datetime import datetime

# Request
import requests

# Pandas
import pandas as pd
from pandas import DataFrame

# Utils
from utils.interface import PipelineInterface
from utils.log_generator import generate_log_line
from utils.transform import (
    remove_emojis,
    extract_text_from_html,
    generate_column_uid
)
from utils.files import save_data, read_data
from utils.db_connector import connect_to_db


class GetOnBoardPipeline(PipelineInterface):

    def __init__(self):
        self.get_on_board_url = "https://www.getonbrd.com/api/v0/categories/programming/jobs?per_page=100&page="
        self.now = datetime.now()
        self.file_name = f'{self.now.strftime("%Y-%m-%d")}_get_on_board.csv'
        self.seniorities = None
        self.old_positions = None
        self.connection = connect_to_db()

    def execute(self):
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        """
        This method is used to extract the data from the API.
        """
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
        """
        This method is used to transform the data.
        """
        df_raw = read_data(self.file_name, 'raw')
        self._get_seniority()
        df_cleaned = self._clean_job_offers(df_raw)
        return save_data(df_cleaned, self.file_name, 'interim')

    def load(self):
        """
        This method is used to load the data into the database and drop the duplicates.
        """
        df_interim = read_data(self.file_name, 'interim')
        df_interim = pd.concat([df_interim, self.old_positions])
        df_interim.drop(columns=[
            'company_name',
            'category_name',
            'projects',
            'functions',
            'seniority',
            'perks',
            'country',
            'remote_modality',
        ], inplace=True)
        df_interim.to_sql(con=self.connection, name='position', if_exists='append', index=False)
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

    def _get_company(self, company_name: str) -> str:
        """
        This method is used to get the company name from the company name.

        :param company_name: The company name.
        :return: The company name.
        """
        company = self.connection.execute(
            f"SELECT id_company FROM company WHERE name = '{company_name}'"
        ).fetchone()
        if company is None:
            self.connection.execute(
                f"INSERT INTO company (name) VALUES ('{company_name}')"
            )
            return self._get_company(company_name)

        return company[0]

    def _get_seniority(self):
        """
        This method is used to get the seniority from the database.
        """
        self.seniorities = self.connection.execute(
            'SELECT id_seniority, seniority FROM seniority'
        ).fetchall()

    def _get_seniority_id(self, seniority_name: str) -> int:
        """
        This method is used to get the seniority id from the seniority name.

        :param seniority_name: The seniority name.
        :return: The seniority id.
        """
        for seniority in self.seniorities:
            if seniority[1] == seniority_name.lower():
                return seniority[0]

    def _get_location(self, location: str) -> str:
        """
        This method is used to get the location id from the location name.
        """
        location_id = self.connection.execute(
            f"SELECT id_location FROM location WHERE country = '{location}'"
        ).fetchone()
        if location_id is None:
            self.connection.execute(
                f"INSERT INTO location (country, continent) VALUES ('{location}', 'unknown')"
            )
            return self._get_location(location)
        return location_id[0]

    def _clean_job_offers(self, df: DataFrame) -> DataFrame:
        """
        This method is used to clean the job offers dataframe.

        :param df: The dataframe of the job offers.
        :return: The cleaned dataframe.
        """
        columns_to_rename = {
            'attributes.title': 'position_title',
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
            'links.public_url': 'position_url',
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
        df_data['currency_id'] = 2
        df_data['activate'] = True
        df_data['position_category_id'] = 9
        df_data['english'] = 'true'
        df_data['english_level'] = 'conversational'

        # Clean the projects, functions and description columns to remove emojis
        df_data[['projects', 'description', 'functions']] = df_data[[
            'projects',
            'description',
            'functions'
        ]].applymap(remove_emojis)

        # Add a column with the unique id of the job offer
        df_data = generate_column_uid(df_data, 'position_url')
        df_old_positions = pd.read_sql('SELECT uid FROM position', con=self.connection)

        df_data = df_data[~df_data.uid.isin(df_old_positions.uid)]

        # Assign the company id
        df_data['company_id'] = df_data['company_name'].apply(self._get_company)

        # Assign the seniority id
        df_data['seniority_id'] = df_data['seniority'].apply(self._get_seniority_id)

        # Assign the job location id
        df_data['location_id'] = df_data['country'].apply(self._get_location)

        # Assign the salary depending on the salary_min and salary_max
        df_data['salary'] = df_data[['salary_min', 'salary_max']].mean(axis=1)
        return df_data

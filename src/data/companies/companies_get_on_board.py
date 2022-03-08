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
from utils.files import save_data, read_data
from utils.transform import extract_text_from_html
from utils.db_connector import connect_to_db


class CompaniesGetOnBoardPipeline(PipelineInterface):

    def __init__(self):
        self.companies_url = 'https://www.getonbrd.com/api/v0/companies?per_page=100&page='
        self.now = datetime.now()
        self.file_name = f'{self.now.strftime("%Y-%m-%d")}_companies_get_on_board.csv'

    def execute(self):
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        df = pd.DataFrame()
        for page in range(1, 10):
            new_data = self._request_companies(page)
            if new_data is not None:
                df_temp = pd.json_normalize(new_data, max_level=None)
                df = pd.concat([df, df_temp])
            else:
                break
        return save_data(df, self.file_name, 'raw')
        pass

    def transform(self):
        df_raw = read_data(self.file_name, 'raw')
        df_cleaned = self._clean_companies(df_raw)
        return save_data(df_cleaned, self.file_name, 'interim')

    def load(self):
        df_interim = read_data(self.file_name, 'interim')
        df_processed = self._compare_companies(df_interim)
        if not df_processed.empty:
            return save_data(df_processed, self.file_name, 'processed')

    def _compare_companies(self, df_interim: DataFrame) -> DataFrame:
        """
        This method is used to compare the companies with the ones in the database.

        :param df_interim: The interim data.
        :return: The processed data.
        """
        engine = connect_to_db()
        df_companies = pd.read_sql_table('company', engine, columns=['name', 'description', 'website'])
        # compare the two dataframes and return the difference in a new dataframe (df_diff)
        df_diff = pd.concat([df_companies, df_interim]).drop_duplicates(subset='name', keep=False)
        if not df_diff.empty:
            df_diff.to_sql('company', engine, if_exists='append', index=False)
        return df_diff

    def _request_companies(self, page_number: int) -> list or None:
        """
        This method is used to request the companies from the API.

        :param page_number: The page number of the companies.
        :return: JSON data of the companies.
        """
        url = f'{self.companies_url}{page_number}'
        payload = {'content-type': 'application/json'}

        response = requests.request("GET", url, data=payload)
        generate_log_line(response)
        content = json.loads(response.text)

        if len(content['data']) == 0:
            return None
        return content['data']

    def _clean_companies(self, df_raw: DataFrame) -> DataFrame:
        """
        This method is used to clean the raw data.

        :param df_raw: The raw data.
        :return: The cleaned data.
        """
        columns_to_rename = {
            'attributes.name': 'name',
            'attributes.projects': 'description',
            'attributes.web': 'website',
        }

        # Select only the columns we need to keep and rename them
        columns_company = list(columns_to_rename.keys())
        df_data = df_raw.loc[:, columns_company]

        df_data = df_data.convert_dtypes()

        # Extract text from html
        df_data[['attributes.projects']] = df_data[['attributes.projects']].applymap(extract_text_from_html)
        df_data['company_premium'] = 'false'

        # Rename columns
        df_data.rename(columns=columns_to_rename, inplace=True)

        return df_data

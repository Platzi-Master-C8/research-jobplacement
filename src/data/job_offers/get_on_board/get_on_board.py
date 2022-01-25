# Python
import json
import re

# Request
import requests

# Pandas
import pandas as pd

# BeautifulSoup
from bs4 import BeautifulSoup


class GetOnBoard:
    def __init__(self):
        self.url = 'https://www.getonbrd.com/api/v0/categories/programming/jobs?per_page=100&page='
        self.page = 1
        self.data = []
        self.df = pd.DataFrame()
        self.get_all_data()
        self.cleaning_job_offers()

    def request_job_offers(self, page_number: int):
        url = f'{self.url}{page_number}&expand=["company", "modality", "seniority"]'
        payload = {'content-type': 'application/json'}

        response = requests.request("GET", url, data=payload)
        content = json.loads(response.text)
        if len(content['data']) == 0:
            return None
        return content['data']

    def remove_emojis(self, text: str):
        regrex_pattern = re.compile(pattern="["
                                            u"\U0001F600-\U0001F64F"  # emoticons
                                            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                            u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                            "]+", flags=re.UNICODE)
        return regrex_pattern.sub(r'', text)

    def get_all_data(self):
        for i in range(1, 10):
            new_data = self.request_job_offers(i)
            if new_data is not None:
                df_temp = pd.json_normalize(new_data, max_level=None)
                self.df = pd.concat([self.df, df_temp])
            else:
                break

    def _get_text_t(self, column_to_clean):
        text = BeautifulSoup(column_to_clean).get_text()
        return text

    def cleaning_job_offers(self):
        df = self.df
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
            'links.public_url': 'public_url',
        }

        columns_job = list(columns_to_rename.keys())
        columns_to_clean = ['attributes.description', 'attributes.projects', 'attributes.functions']
        df_data = df.loc[:, columns_job]
        df_data.loc[:, columns_to_clean] = df_data.loc[:, columns_to_clean].applymap(self._get_text_t)
        df_data.rename(columns=columns_to_rename, inplace=True)
        df_data.reset_index(inplace=True, drop=True)
        df_data = df_data.convert_dtypes()
        df_data['salary_type'] = 'USD'
        df_data['activate'] = True
        df_data[['projects', 'description', 'functions']] = df_data[['projects', 'description', 'functions']].applymap(
            self.remove_emojis)
        return df_data

    def _get_text_t(self, column_to_clean):
        text = BeautifulSoup(column_to_clean).get_text()
        return text

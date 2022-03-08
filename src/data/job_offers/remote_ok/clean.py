# Python
import datetime as dt

# Numpy
import numpy as np

# Pandas
import pandas as pd
from pandas import DataFrame

# Utils
from utils.files import save_data, read_data
from utils.transform import remove_emojis


class Clean:
    """
    Cleaning the data from remote_ok.com website.
    """

    def __init__(self):
        today = dt.date.today()
        self.file_name = f'REMOTEOK_{today}_offers.csv'
        self.raw_data = read_data(self.file_name, 'raw')
        self.clean_data(self.raw_data)

    def clean_data(self, data: DataFrame):
        """
        Cleans the data and returns a dataframe with the cleaned data

        :param data: DataFrame with the raw data
        """
        if not data.empty:
            df = data
            # Remove the emoji from the salary
            df['Salario'] = df['Salario'].str.replace('💰 ', '').str.replace('$', '').str.replace('*', '').str.replace(
                'k', '000')
            # Parse the salaries
            df[['MIN Salario', 'MAX Salario']] = df['Salario'].str.split(' - ', 1, expand=True)
            df['MAX Salario'] = pd.to_numeric(df['MAX Salario'])
            df['MIN Salario'] = pd.to_numeric(df['MIN Salario'])
            df['AVG Salario'] = (df['MAX Salario'] + df['MIN Salario']) / 2

            # Drop the salary column
            df = df.drop(['Salario'], axis=1)
            df['currency'] = 'USD'
            df['Sallary period'] = 'Year'
            df['Descripción'] = df['Descripción'].apply(remove_emojis)
            df['Ubicacion'] = df['Ubicacion'].apply(remove_emojis)
            df['Ubicacion'] = df['Ubicacion'].str.replace('🌏 ', '')

            # Remove emojis from the description
            df['Descripción'] = df['Descripción'].str.replace('[', '').str.replace(r'\\n', ' ').str.replace(
                ']', '').str.replace('🔎', '').str.replace("\\", ' ').str.replace('✅', '').str.replace(
                '"', '').str.replace('🌏 ', '').str.replace(' 👋', '').str.replace("'", '´').str.strip()

            description_slited = []
            descriptions = list(df['Descripción'])
            for text in descriptions:
                description_slited.append(' '.join(text.split()))

            df['Descripción'] = description_slited
            today = dt.date.today()

            df.columns = ['Position', 'Company', 'Location', 'Date_published',
                          'URL', 'Description', 'SKILLS', 'Home_URL',
                          'Site_Name', 'MIN_SALARY', 'MAX_SALARY',
                          'MIDPOINT_SALARY', 'CURRENCY', 'SALLARY_PERIOD'
                          ]

            df['Date_published'] = df['Date_published'].str.replace('T', ' ')
            df['Company'] = df['Company'].str.upper()
            df['Position'] = df['Position'].str.upper()

            stacks_frontend = ['JAVASCRIPT', 'FRONTEND', 'FRONT END', 'REACT', ' UI', ' UX']
            stacks_backend = ['RUBY ON RAILS', 'PYTHON', 'BACKEND', 'BACK END', 'PHP', 'GO DEVELOPER', 'GO', 'API ']
            stacks_manager = ['MANAGER', 'MANAGMENT']
            stacks_fullstack = ['FULL STACK', 'FULLSTACK']
            stacks_analytics = ['ANALYST', 'ANALYTICS', 'BUSSINES INTELLIGENCE']
            stacks_developers = ['DEVELOPER', 'SOFTWARE', 'WEB', 'APPLICATION', 'SOLUTIONS']

            conditions = [
                df['Position'].str.contains('|'.join(stacks_frontend)),
                df['Position'].str.contains('|'.join(stacks_backend)),
                df['Position'].str.contains('|'.join(stacks_manager)),
                df['Position'].str.contains('|'.join(stacks_fullstack)),
                df['Position'].str.contains('|'.join(stacks_analytics)),
                df['Position'].str.contains('DEVOPS'),
                df['Position'].str.contains('EDITOR'),
                df['Position'].str.contains('ECONOMIST'),
                df['Position'].str.contains('BLOCKCHAIN'),
                df['Position'].str.contains('WRAITER'),
                df['Position'].str.contains('EDITOR'),
                df['Position'].str.contains('CONSULTANT'),
                df['Position'].str.contains('SUPPORT'),
                df['Position'].str.contains('DATA'),
                df['Position'].str.contains('ECONOMIST'),
                df['Position'].str.contains('RECRUITER'),
                df['Position'].str.contains('HR'),
                df['Position'].str.contains('|'.join(stacks_developers)),
            ]

            choices = [
                'FRONTEND',
                'BACKEND',
                'MANAGER',
                'FULLSTACK',
                'ANALYST',
                'DEVOPS',
                'EDITOR',
                'ECONOMIST',
                'BLOCKCHAIN',
                'WRAITER',
                'EDITOR',
                'CONSULTANT',
                'CUSTOMER SUPPORT',
                'DATA',
                'ECONOMIST',
                'RECRUITER',
                'HUMAN RESOURCES',
                'DEVELOPER',
            ]

            df['categories'] = np.select(conditions, choices, 'UNKNOWN')

            df['CURRENCY'] = df['CURRENCY'].replace('USD', 2)
            df['seniority'] = 4
            df['modality'] = 'unknown'
            df[['activate', 'english', 'remote']] = 'True'
            df['english_level'] = 'conversational'
            df[['num_offers']] = 0
            save_data(df, self.file_name, 'interim')
        else:
            print('The dataframe is empty')

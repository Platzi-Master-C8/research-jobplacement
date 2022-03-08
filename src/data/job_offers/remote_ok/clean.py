# Python
import datetime as dt
import re
import hashlib

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
                df['Position'].str.upper().str.contains('SENIOR'),
                df['Position'].str.upper().str.contains('JUNIOR'),
                df['Position'].str.upper().str.contains('MID'),
                df['Position'].str.upper().str.contains('TRAINEE'),
            ]

            choices = [
                1,  # 'SENIOR'
                2,  # 'MID'
                3,  # 'JUNIOR'
                5,  # 'TRAINEE'
            ]

            df['seniority'] = np.select(conditions, choices, 4)

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

            df['Location'] = df['Location'].str.strip()

            df['categories'] = np.select(conditions, choices, '-')
            df['CURRENCY'] = df['CURRENCY'].replace('USD', 3)
            df['modality'] = 'full-time'
            df[['activate', 'english', 'remote']] = 'True'
            df['english_level'] = 'conversational'
            df[['num_offers']] = 1
            df = df.drop_duplicates()
            df = df[~df['SKILLS'].str.contains('exec')]

            uids = (df
                    .apply(lambda row: hashlib.md5(bytes(row['URL'].encode())), axis=1)
                    .apply(lambda hash_object: hash_object.hexdigest())
                    )
            df['uid'] = uids

            df = self.location_unify(df)

            df.to_csv(f'./data/processed/REMOTEOK_{today}_offers_CLEAN.csv',
                      encoding='utf-8-sig', index=False)
        else:
            print('The dataframe is empty')

    def remove_emoji(self, string):
        """
        Remove emoji from string (unicode)
        """
        emoji_pattern = re.compile("["
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                   u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                   u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                   u"\U00002500-\U00002BEF"  # chinese char
                                   u"\U00002702-\U000027B0"
                                   u"\U00002702-\U000027B0"
                                   u"\U000024C2-\U0001F251"
                                   u"\U0001f926-\U0001f937"
                                   u"\U00010000-\U0010ffff"
                                   u"\u2640-\u2642"
                                   u"\u2600-\u2B55"
                                   u"\u200d"
                                   u"\u23cf"
                                   u"\u23e9"
                                   u"\u231a"
                                   u"\ufe0f"  # dingbats
                                   u"\u3030"
                                   "]+", flags=re.UNICODE)
        return emoji_pattern.sub(r'', string)

    def location_unify(self, df):
        """
        Unify location names to the most common ones in the dataset
        """
        df['Location'] = df['Location'].str.replace('All Around the World', 'Worldwide')
        df['Location'] = df['Location'].str.replace('Anywhere', 'Worldwide')
        df['Location'] = df['Location'].str.replace('Anywhere in the US', 'USA')
        df['Location'] = df['Location'].str.replace('Austin, Texas', 'Austin, Texas')
        df['Location'] = df['Location'].str.replace('Austin, TX', 'Austin, Texas')
        df['Location'] = df['Location'].str.replace('Boston', 'Boston, Massachusetts, United States')
        df['Location'] = df['Location'].str.replace('Boston, Massachusetts, United States',
                                                    'Boston, Massachusetts, United States')
        df['Location'] = df['Location'].str.replace('Cet', 'CET Timezone')
        df['Location'] = df['Location'].str.replace('CET Timezone', 'CET Timezone')
        df['Location'] = df['Location'].str.replace('Chicago, IL', 'Chicago, Illinois, United States')
        df['Location'] = df['Location'].str.replace('Chicago, Illinois, United States',
                                                    'Chicago, Illinois, United States')
        df['Location'] = df['Location'].str.replace('Dallas, Texas, United States', 'Dallas, Texas, United States')
        df['Location'] = df['Location'].str.replace('Dallas, TX', 'Dallas, Texas, United States')
        df['Location'] = df['Location'].str.replace('EU', 'USA')
        df['Location'] = df['Location'].str.replace('London', 'London, United Kingdom')
        df['Location'] = df['Location'].str.replace('London, England', 'London, United Kingdom')
        df['Location'] = df['Location'].str.replace('London, United Kingdom', 'London, United Kingdom')
        df['Location'] = df['Location'].str.replace('Los Angeles, CA', 'Los Angeles, California')
        df['Location'] = df['Location'].str.replace('Los Angeles, CA or Remote', 'Los Angeles, California')
        df['Location'] = df['Location'].str.replace('Los Angeles, California', 'Los Angeles, California')
        df['Location'] = df['Location'].str.replace('Los Angeles, California, United States', 'Los Angeles, California')
        df['Location'] = df['Location'].str.replace('Miami, FL', 'Miami, Florida')
        df['Location'] = df['Location'].str.replace('Miami, Florida', 'Miami, Florida')
        df['Location'] = df['Location'].str.replace('New York', 'New York, NY')
        df['Location'] = df['Location'].str.replace('New York City Metropolitan Area', 'New York, NY')
        df['Location'] = df['Location'].str.replace('New York City, New York, United States', 'New York, NY')
        df['Location'] = df['Location'].str.replace('New York or Us/eu Timezones', 'New York, NY')
        df['Location'] = df['Location'].str.replace('New York, NY', 'New York, NY')
        df['Location'] = df['Location'].str.replace('New York, NY (Remote)', 'New York, NY')
        df['Location'] = df['Location'].str.replace('New York, Ny/remote', 'New York, NY')
        df['Location'] = df['Location'].str.replace('New York, US', 'New York, NY')
        df['Location'] = df['Location'].str.replace('Remote - United States', 'USA')
        df['Location'] = df['Location'].str.replace('Remote - United States Only', 'USA')
        df['Location'] = df['Location'].str.replace('Remote - US', 'USA')
        df['Location'] = df['Location'].str.replace('Remote US', 'USA')
        df['Location'] = df['Location'].str.replace('Remote, United States', 'USA')
        df['Location'] = df['Location'].str.replace('Remote, US', 'USA')
        df['Location'] = df['Location'].str.replace('Remote, USA', 'USA')
        df['Location'] = df['Location'].str.replace('Reston, Virginia, United States', 'USA')
        df['Location'] = df['Location'].str.replace('San Francisco CA / US', 'San Francisco, California')
        df['Location'] = df['Location'].str.replace('San Francisco, CA', 'San Francisco, California')
        df['Location'] = df['Location'].str.replace('San Francisco, Ca/remote', 'San Francisco, California')
        df['Location'] = df['Location'].str.replace('San Francisco, California', 'San Francisco, California')
        df['Location'] = df['Location'].str.replace('San Francisco, California, United States',
                                                    'San Francisco, California')
        df['Location'] = df['Location'].str.replace('Toronto', 'Toronto, Ontario, Canada')
        df['Location'] = df['Location'].str.replace('Toronto, Ontario', 'Toronto, Ontario, Canada')
        df['Location'] = df['Location'].str.replace('Toronto, Ontario, Canada', 'Toronto, Ontario, Canada')
        df['Location'] = df['Location'].str.replace('U.s. Remote', 'USA')
        df['Location'] = df['Location'].str.replace('Uk', 'United Kingdom')
        df['Location'] = df['Location'].str.replace('Uk/us/remote', 'United Kingdom')
        df['Location'] = df['Location'].str.replace('United Kingdom', 'United Kingdom')
        df['Location'] = df['Location'].str.replace('United Kingdom (Anywhere)', 'United Kingdom')
        df['Location'] = df['Location'].str.replace('United States - Remote', 'USA')
        df['Location'] = df['Location'].str.replace('United States, Canada', 'United States, Canada')
        df['Location'] = df['Location'].str.replace('United States, Remote', 'USA')
        df['Location'] = df['Location'].str.replace('US', 'USA')
        df['Location'] = df['Location'].str.replace('US - Remote', 'USA')
        df['Location'] = df['Location'].str.replace('US Only', 'USA')
        df['Location'] = df['Location'].str.replace('US Remote', 'USA')
        df['Location'] = df['Location'].str.replace('usa', 'USA')
        df['Location'] = df['Location'].str.replace('USA and Canada', 'United States, Canada')
        df['Location'] = df['Location'].str.replace('US-only', 'USA')
        df['Location'] = df['Location'].str.replace('Us-Remote', 'USA')
        df['Location'] = df['Location'].str.replace('Vancouver', 'Vancouver, BC')
        df['Location'] = df['Location'].str.replace('Vancouver, BC', 'Vancouver, BC')
        df['Location'] = df['Location'].str.replace('Worldwide', 'Worldwide')
        df['Location'] = df['Location'].str.replace('Worldwide (US Hours)', 'Worldwide')

        return df

from os import remove
import pandas as pd
import numpy as np
import datetime as dt
import re
import hashlib


class Clean():
    def __init__(self):
        today = dt.date.today()
        route = f'./data/raw/REMOTEOK_{today}_offers.csv'
        self.raw_data = self.read_raw(route)
        self.clean_data = self.clean_data(self.raw_data)

    def read_raw(self, route):
        try:
            return pd.read_csv(route)
        except:
            print('Andala Osa el archivo no existe')
            return None

    def clean_data(self, data):
        if not data.empty:
            df = data
            df['Salario'] = df['Salario'].str.replace('💰 ', ''
                                        ).str.replace('$', ''
                                        ).str.replace('*', ''
                                        ).str.replace('k', '000'
                                        )

            df[['MIN Salario', 'MAX Salario']] = df['Salario'].str.split(
                ' - ', 1, expand=True)
            df['Salario']
            df['MAX Salario'] = pd.to_numeric(df['MAX Salario'])
            df['MIN Salario'] = pd.to_numeric(df['MIN Salario'])
            df['AVG Salario'] = (df['MAX Salario'] + df['MIN Salario']) / 2
            df = df.drop(['Salario'], axis=1)
            df['currency'] = 'USD'
            df['Sallary period'] = 'Year'
            df['Descripción'] = df['Descripción'].apply(self.remove_emoji)
            df['Ubicacion'] = df['Ubicacion'].apply(self.remove_emoji)
            df['Ubicacion'] = df['Ubicacion'].str.replace('🌏 ', '')
            # print(df['Descripción'])
            df['Descripción'] = df['Descripción'].str.replace('[', ''
                                   ).str.replace(r'\\n', ' '
                                   ).str.replace(']', ''
                                   ).str.replace('🔎', ''
                                   ).str.replace("\\", ' '
                                   ).str.replace('✅', ''
                                   ).str.replace('"', ''
                                   ).str.replace('🌏 ', ''
                                   ).str.replace(' 👋', ''
                                   ).str.replace("'", '´'
                                   ).str.strip()

            a = []
            b = list(df['Descripción'])
            for text in b:
                a.append(' '.join(text.split()))

            df['Descripción'] = a
            today = dt.date.today()
            # print(df.columns)

            df.columns = ['Position', 'Company', 'Location', 'Date_published',
                          'URL', 'Description', 'SKILLS','Home_URL',
                          'Site_Name', 'MIN_SALARY', 'MAX_SALARY', 
                          'MIDPOINT_SALARY', 'CURRENCY', 'SALLARY_PERIOD'
                          ]

            df['Date_published'] = df['Date_published'].str.replace('T', ' ')
            df['Company'] = df['Company'].str.upper()
            df['Position'] = df['Position'].str.upper()

            front = ['JAVASCRIPT','FRONTEND', 'FRONT END', 'REACT', ' UI',' UX']
            back = ['RUBY ON RAILS', 'PYTHON', 'BACKEND', 'BACK END', 'PHP',
                       'GO DEVELOPER', 'GO', 'API ']
            mng = ['MANAGER','MANAGMENT']
            full = ['FULL STACK','FULLSTACK']
            aly = ['ANALYST','ANALYTICS', 'BUSSINES INTELLIGENCE']
            unk = ['DEVELOPER','SOFTWARE','WEB','APPLICATION', 'SOLUTIONS']

            conditions = [
                df['Position'].str.upper().str.contains('SENIOR'),
                df['Position'].str.upper().str.contains('JUNIOR'),
                df['Position'].str.upper().str.contains('MID'),
                df['Position'].str.upper().str.contains('TRAINEE'),
            ]

            choices = [
                1, #'SENIOR'
                2, #'MID'
                3, #'JUNIOR'
                5, #'TRAINEE'
            ]

            df['seniority'] = np.select(conditions, choices,4)

            conditions = [
                df['Position'].str.contains('|'.join(front)),
                df['Position'].str.contains('|'.join(back)),
                df['Position'].str.contains('|'.join(mng)),
                df['Position'].str.contains('|'.join(full)),
                df['Position'].str.contains('|'.join(aly)),
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
                df['Position'].str.contains('|'.join(unk)),
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
            
            df['categories'] = np.select(conditions, choices,'-')
            # df = df.drop_duplicates()
            #USD = 3

            df['CURRENCY'] = df['CURRENCY'].replace('USD', 3)
            df['modality'] = 'full-time'
            df[['activate', 'english', 'remote']] = 'True'
            df['english_level'] = 'conversational'
            df[['num_offers']] = 1
            df = df.drop_duplicates()
            df = df[~df['SKILLS'].str.contains('exec')]

            uids = (df
                    .apply(lambda row : hashlib.md5(bytes(row['URL'].encode())), axis=1)
                    .apply(lambda hash_object : hash_object.hexdigest())
                    )
            df['uid'] = uids

            df = self.location_unify(df)

            df.to_csv(f'./data/processed/REMOTEOK_{today}_offers_CLEAN.csv',
                      encoding='utf-8-sig', index=False)
        else:
            print('No existen datos que limpiar')

    def remove_emoji(self,string):
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
        df['Location'] = df['Location'].str.replace('All Around the World', 'Worldwide')
        df['Location'] = df['Location'].str.replace('Anywhere', 'Worldwide')
        df['Location'] = df['Location'].str.replace('Anywhere in the US', 'USA')
        df['Location'] = df['Location'].str.replace('Austin, Texas', 'Austin, Texas')
        df['Location'] = df['Location'].str.replace('Austin, TX', 'Austin, Texas')
        df['Location'] = df['Location'].str.replace('Boston', 'Boston, Massachusetts, United States')
        df['Location'] = df['Location'].str.replace('Boston, Massachusetts, United States', 'Boston, Massachusetts, United States')
        df['Location'] = df['Location'].str.replace('Cet', 'CET Timezone')
        df['Location'] = df['Location'].str.replace('CET Timezone', 'CET Timezone')
        df['Location'] = df['Location'].str.replace('Chicago, IL', 'Chicago, Illinois, United States')
        df['Location'] = df['Location'].str.replace('Chicago, Illinois, United States', 'Chicago, Illinois, United States')
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
        df['Location'] = df['Location'].str.replace('San Francisco, California, United States', 'San Francisco, California')
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


if __name__ == '__main__':
    a = Clean()

from os import remove
import pandas as pd
import numpy as np
import datetime as dt
import re


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
            
            df['categories'] = np.select(conditions, choices,'UNKNOWN')
            # df = df.drop_duplicates()

            df['CURRENCY'] = df['CURRENCY'].replace('USD', 2)
            df['seniority'] = 4
            df['modality'] = 'unknown'
            df[['activate', 'english', 'remote']] = 'True'
            df['english_level'] = 'conversational'
            df[['num_offers']] = 0

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


if __name__ == '__main__':
    a = Clean()

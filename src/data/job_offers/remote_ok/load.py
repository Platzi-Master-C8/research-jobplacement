# Python
import datetime as dt

# Pandas
import hashlib

import pandas as pd

# SQLAlchemy
from pandas import DataFrame

# Connection
from utils.db_connector import connect_to_db

# Utils
from utils.files import read_data, save_data

pd.set_option('display.max_columns', 500)


class Load:
    """
    Load data from remote_ok.com into database
    """

    def __init__(self) -> None:
        self.today = dt.date.today()
        self.file_name = f'REMOTEOK_{self.today}_offers.csv'
        self.data = read_data(self.file_name, 'interim')
        self.con = connect_to_db()
        self.categories = self.insert_category(self.data["categories"].drop_duplicates(),
                                               self.con)
        self.companies = self.insert_company(self.data['Company'].drop_duplicates(),
                                             self.con)
        self.locations = self.insert_location(self.data['Location'].drop_duplicates(),
                                              self.con)
        self.ins, self.position = self.insert_data(self.data, self.categories,
                                                   self.companies, self.locations,
                                                   self.con)
        self.skills = self.insert_skills(self.data['SKILLS'],
                                         self.con)
        self.insert_position_skills(self.ins, self.skills, self.position,
                                    self.con)

    def get_data(self, table: str, con) -> DataFrame:
        """
        Get data from table in database

        :param table: table to get data
        :param con: connection to database
        :return: dataframe with data
        """
        conn = con
        sql_df = pd.read_sql(f'SELECT * FROM {table}', con=conn)
        return sql_df

    def find_duplicates(self, column: str, key: str, key_value: str, con: str) -> DataFrame:
        """
        Find duplicates in column and return dataframe with duplicates

        :param column: column to find duplicates
        :param key: key to find duplicates
        :param key_value: value of key to find duplicates
        :param con: connection to database
        :return: dataframe with duplicates
        """
        conn = con
        query = f"SELECT * FROM {column} WHERE {key} = '{key_value}'"
        df = pd.read_sql(query, con=conn)
        return df

    def insert_company(self, df: pd.DataFrame, con) -> DataFrame:
        """
        Insert companies in database and return dataframe with companies

        :param df: dataframe with companies
        :param con: connection to database
        :return: dataframe with companies
        """
        conn = con
        df = pd.DataFrame({'name': df})
        sql_df = self.get_data('company', conn)
        currents = list(df['name'])
        registered = list(sql_df['name'].str.upper())
        news = pd.DataFrame({'name': list(set(currents) - set(registered))})
        if news.empty:
            print('The companies are already in the database')
        else:
            news['company_premium'] = 'false'
            news['description'] = 'None'
            news = pd.DataFrame(news)
            news.to_sql('company', con=conn,
                        if_exists='append',
                        index=False)
            print(f'Companies inserted: {len(news)}')

        sql_df = self.get_data('company', conn)

        return sql_df[['id_company', 'name']]

    def insert_category(self, df: DataFrame, con) -> DataFrame:
        """
        Insert categories in database and return dataframe with categories

        :param df: dataframe with categories
        :param con: connection to database
        :return: dataframe with categories
        """
        conn = con
        df = pd.DataFrame({'category': df})
        sql_df = self.get_data('position_category', con)
        current_cats = list(df['category'])
        registered_cats = list(sql_df['category'].str.upper())
        new_cats = pd.DataFrame(
            {'category': list(set(current_cats) - set(registered_cats))})
        if new_cats.empty:
            print('The categories are already in the database')
        else:
            new_cats.to_sql('position_category', con=conn,
                            if_exists='append',
                            index=False)
            print(f'Categories inserted: {len(new_cats)}')

        sql_df = self.get_data('position_category', con)
        return sql_df

    def insert_location(self, df: DataFrame, con):
        """
        Insert locations in database and return dataframe with locations

        :param df: dataframe with locations
        :param con: connection to database
        :return: dataframe with locations
        """
        df = pd.DataFrame({'country': df})
        currents = list(df['country'])
        conn = con
        sql_df = self.get_data('location', conn)

        registered = list(sql_df['country'])
        news = pd.DataFrame({'country': list(set(currents) - set(registered))})

        if news.empty:
            print('The locations are already in the database')
        else:
            news['continent'] = 'unknown'
            news = pd.DataFrame(news)
            news.to_sql('location', con=conn,
                        if_exists='append',
                        index=False)
            print(f'Locations inserted: {len(news)}')
        sql_df = self.get_data('location', conn)
        return sql_df[['id_location', 'country']]

    def insert_data(self,
                    df: DataFrame,
                    categories: DataFrame,
                    companies: DataFrame,
                    locations: DataFrame,
                    con
                    ) -> tuple:
        """
        Insert data in database and return dataframe with data

        :param df: dataframe with data
        :param categories: dataframe with categories
        :param companies: dataframe with companies
        :param locations: dataframe with locations
        :param con: connection to database
        :return: dataframe with data
        """
        comp_id = list(companies['id_company'])
        comp_nm = list(companies['name'])
        cat_id = list(categories['id_position_category'])
        cat_nm = list(categories['category'])
        loc_id = list(locations['id_location'])
        loc_nm = list(locations['country'])

        # REMPLAZO DE NOMBRES POR IDS
        for company in range(len(comp_id)):
            df['Company'] = df['Company'].replace(
                comp_nm[company], comp_id[company])

        for category in range(len(cat_id)):
            df['categories'] = df['categories'].replace(
                cat_nm[category], cat_id[category])

        for location in range(len(loc_id)):
            df['Location'] = df['Location'].replace(
                loc_nm[location], loc_id[location])

        # Order columns and drop duplicates
        df = df[['Position', 'categories', 'seniority',
                 'Description', 'modality', 'Date_published',
                 'activate', 'num_offers', 'MIN_SALARY',
                 'MAX_SALARY', 'MIDPOINT_SALARY', 'CURRENCY',
                 'remote', 'Location', 'english', 'english_level',
                 'URL', 'Company', 'SKILLS']]

        names = ['position_title', 'position_category_id',
                 'seniority_id', 'description', 'modality',
                 'date_position', 'activate', 'num_offers',
                 'salary_min', 'salary_max', 'salary',
                 'currency_id', 'remote', 'location_id',
                 'english', 'english_level', 'position_url',
                 'company_id', 'skill']

        df.columns = names
        df['date_position'] = df['date_position'].str[:-6:]
        df2 = df.copy()
        df = df.drop(['skill'], axis=1)

        registered = self.get_data('position', con).drop(
            ['id_position', 'uid'], axis=1)

        registered['date_position'] = registered['date_position'].astype('str')
        registered['remote'] = registered['remote'].astype('str')
        registered['salary'] = registered['salary'].astype('float64')

        reg = list(registered['position_url'])
        dfs = list(df['position_url'])
        new_cats = pd.DataFrame({'position_url': list(set(dfs) - set(reg))})
        news = df[df['position_url'].isin(new_cats['position_url'])]
        news = news.drop_duplicates()

        uids = (df
                .apply(lambda row: hashlib.md5(bytes(row['position_url'].encode())), axis=1)
                .apply(lambda hash_object: hash_object.hexdigest())
                )
        df['uid'] = uids

        if news.empty:
            print('The positions are already in the database')
        else:
            conn = con
            df.to_sql('position', con=conn,
                      if_exists='append',
                      index=False)
            print(f'Positions inserted: {len(news)}')

        registered = self.get_data(
            'position', con).drop(
            ['uid'], axis=1).drop_duplicates()

        return df2, registered

    def insert_skills(self, df: DataFrame, con):
        """
        Insert skills into the database and return dataframe with data

        :param df: dataframe with data
        :param con: connection to database
        """
        df = list(df)
        df = [x.replace('[', '').replace(']', '').replace("'", '').strip().split(', ') for x in df]
        df = [item for sublist in df for item in sublist]
        df = pd.DataFrame({'skill': df}).drop_duplicates()
        currents = list(df['skill'])

        # Get skills from database
        conn = con
        sql_df = self.get_data('skill', con)
        registered = list(sql_df['skill'])
        news = pd.DataFrame({'skill': list(set(currents) - set(registered))})

        if news.empty:
            print('Skills already exist in database')
        else:
            news.to_sql('skill', con=conn,
                        if_exists='append',
                        index=False)
            print(f'Skills added: {len(news)}')

        sql_df = self.get_data('skill', con)

        return sql_df

    def insert_position_skills(self, df: DataFrame, skill_: DataFrame, keys: DataFrame, con):
        """
        Insert position skills into the database and return dataframe with data

        :param df: dataframe with data
        :param skill_: dataframe with skills
        :param keys: list of keys
        :param con: connection to database
        """
        keys['date_position'] = keys['date_position'].astype('str')
        keys['remote'] = keys['remote'].astype('str')
        keys['salary'] = keys['salary'].astype('float64')
        df2 = df.merge(keys, how='left', on=[
            'position_title', 'date_position',
            'position_url'
        ])
        df2 = df2[['id_position', 'skill']]
        df2['skill'] = [x.replace('[', '').replace(']', '').replace(
            "'", '').strip().split(', ') for x in df2['skill']]

        positions = []
        skills = []

        for x in range(len(df2)):
            for skill in df2['skill'][x]:
                pos = df2['id_position'][x]
                positions.append(pos)
                skills.append(skill)

        df2 = pd.DataFrame({'position_id': positions, 'skill_id': skills})
        df2 = df2.drop_duplicates().dropna()

        registered = self.get_data('position_skill', con).drop(
            ['id_position_skill'], axis=1)

        skl_id = list(skill_['id_skill'])
        skl_nm = list(skill_['skill'])

        for x in range(len(skl_id)):
            df2['skill_id'] = df2['skill_id'].replace(
                skl_nm[x], skl_id[x])

        df2['key'] = df2['position_id'].astype('str') + df2['skill_id'].astype('str')
        registered['key'] = registered['position_id'].astype('str') + registered['skill_id'].astype('str')

        reg = list(registered['key'])
        dfs = list(df2['key'])

        new_cats = pd.DataFrame(
            {'key': list(set(dfs) - set(reg))})

        news = df2[df2['key'].isin(new_cats['key'])]
        news = news.drop(['key'], axis=1)

        news = news.fillna(1)

        if news.empty:
            print('Position skills already exist in database')
        else:
            conn = con
            news.to_sql('position_skill', con=conn,
                        if_exists='append',
                        index=False)
            save_data(news, self.file_name, 'processed')
            print(f'Position skills added: {len(news)}')



from numpy import dtype
import pandas as pd
import datetime as dt
import time
from sqlalchemy import create_engine
# import remoteok.load.conection as pc
import conection as pc


pd.set_option('display.max_columns', 500)


class Load():
    def __init__(self) -> None:
        self.today = dt.date.today()
        self.route = f'./remoteok/clean_data/REMOTEOK_{self.today}_offers_CLEAN.csv'
        self.data = self.read_file(self.route)
        self.con = pc.connection_elephant()
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
        self.con.close()

    def read_file(self, route: str) -> pd.DataFrame:
        try:
            return pd.read_csv(route)
        except:
            print('Error en la lectura de la datra')
            return None

    def get_data(self, table, con):
        conn = con
        sql_df = pd.read_sql(
            f"""SELECT * FROM {table};
                            """,
            con=conn)
        return sql_df

    def find_duplicates(self, column: str, key: str, key_value: str, con: str):
        conn = con

        query = f"""SELECT * FROM {column}
        WHERE {key} = '{key_value}';"""

        df = pd.read_sql(query,
                         con=conn)

    def insert_company(self, df: pd.DataFrame, con) -> pd.DataFrame:
        conn = con
        df = pd.DataFrame({'name': df})
        sql_df = self.get_data('company', conn)
        currents = list(df['name'])
        registered = list(sql_df['name'].str.upper())
        news = pd.DataFrame({'name': list(set(currents) - set(registered))})
        # print(news)
        if news.empty:
            print('No existen registros nuevos para company')
        else:
            news['ceo'] = 'unknown'
            news['company_premium'] = 'false'
            news['description'] = '--'
            news = pd.DataFrame(news)
            news.to_sql('company', con=conn,
                        if_exists='append',
                        index=False)
            print('se insertaron:', len(news), ' a company')

        sql_df = self.get_data('company', conn)

        return(sql_df[['id_company', 'name']])

    def insert_category(self, df, con):
        conn = con
        df = pd.DataFrame({'category': df})
        sql_df = self.get_data('position_category', con)
        current_cats = list(df['category'])
        registered_cats = list(sql_df['category'].str.upper())
        new_cats = pd.DataFrame(
            {'category': list(set(current_cats) - set(registered_cats))})
        if new_cats.empty:
            print('No existen registros nuevos para position category')
        else:
            new_cats.to_sql('position_category', con=conn,
                            if_exists='append',
                            index=False)
            print('Se registro lo sigiente', len(new_cats),'a category')

        sql_df = self.get_data('position_category', con)
        # conn.close()
        return(sql_df)

    def insert_location(self, df, con):
        df = pd.DataFrame({'country': df})
        currents = list(df['country'])
        conn = con
        sql_df = self.get_data('location', conn)

        registered = list(sql_df['country'])
        news = pd.DataFrame({'country': list(set(currents) - set(registered))})

        if news.empty:
            print('No existen registros nuevosoara location')
        else:
            news['continent'] = 'unknown'
            news = pd.DataFrame(news)
            news.to_sql('location', con=conn,
                        if_exists='append',
                        index=False)
            print('se insertaron: ' + (str)(len(news)),
                  news)
        sql_df = self.get_data('location', conn)
        return(sql_df[['id_location', 'country']])

    def insert_data(self, df, categories, companies, locations, con):
        comp_id = list(companies['id_company'])
        comp_nm = list(companies['name'])
        cat_id = list(categories['id_position_category'])
        cat_nm = list(categories['category'])
        loc_id = list(locations['id_location'])
        loc_nm = list(locations['country'])

        # REMPLAZO DE NOMBRES POR IDS
        for x in range(len(comp_id)):
            df['Company'] = df['Company'].replace(
                comp_nm[x], comp_id[x])

        for x in range(len(cat_id)):
            df['categories'] = df['categories'].replace(
                cat_nm[x], cat_id[x])

        for x in range(len(loc_id)):
            df['Location'] = df['Location'].replace(
                loc_nm[x], loc_id[x])

        # REORDENAMIENTO Y RENOMBRAMIENTO DE COLUMNAS
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
        # registered['activate'] = registered['activate'].astype('str')
        # registered['english'] = registered['english'].astype('str')
        registered['remote'] = registered['remote'].astype('str')
        registered['salary'] = registered['salary'].astype('float64')
        
        news = df[~df.isin(registered)]

        df['key'] = (df['position_title'].astype('str') +
            df['position_category_id'].astype('str') +
            df['seniority_id'].astype('str') +
            df['modality'].astype('str') +
            df['date_position'].astype('str') +
            df['activate'].astype('str') +
            df['num_offers'].astype('str') +
            df['salary_min'].astype('str') +
            df['salary_max'].astype('str') +
            df['salary'].astype('str') +
            df['currency_id'].astype('str') +
            df['remote'].astype('str') +
            df['location_id'].astype('str') +
            df['english'].astype('str') +
            df['english_level'].astype('str') +
            df['position_url'].astype('str') +
            df['company_id'].astype('str'))
        
        registered['key'] = (df['position_title'].astype('str') +
            df['position_category_id'].astype('str') +
            df['seniority_id'].astype('str') +
            df['modality'].astype('str') +
            df['date_position'].astype('str') +
            df['activate'].astype('str') +
            df['num_offers'].astype('str') +
            df['salary_min'].astype('str') +
            df['salary_max'].astype('str') +
            df['salary'].astype('str') +
            df['currency_id'].astype('str') +
            df['remote'].astype('str') +
            df['location_id'].astype('str') +
            df['english'].astype('str') +
            df['english_level'].astype('str') +
            df['position_url'].astype('str') +
            df['company_id'].astype('str') )

        news = df[~df['key'].isin(registered['key'])]
        news = news.drop(['key'],axis=1)
        # print(df[df['key'].isin(registered['key'])])

        if news.empty:
            print('No existen registros nuevos para position')
        else:
            conn = con
            df.to_sql('position', con=conn,
                      if_exists='append',
                      index=False)
            print(f'Se cargaron : {len(news)} registros a Position')

        registered = self.get_data(
            'position', con).drop(
            ['uid'], axis=1)

        return df2, registered

    def insert_skills(self, df, con):
        df = list(df)
        df = [x.replace('[', '').replace(']', '').replace(
            "'", '').strip().split(', ') for x in df]
        df = [item for sublist in df for item in sublist]
        df = pd.DataFrame({'skill': df}).drop_duplicates()
        currents = list(df['skill'])
        conn = con
        sql_df = self.get_data('skill', con)
        registered = list(sql_df['skill'])
        news = pd.DataFrame({'skill': list(set(currents) - set(registered))})

        if news.empty:
            print('No existen registros nuevos en skill')
        else:
            news.to_sql('skill', con=conn,
                        if_exists='append',
                        index=False)
            print('se insertaron: ' + (str)(len(news)),
                  news)

        sql_df = self.get_data('skill', con)

        return sql_df

    def insert_position_skills(self, df, skill_, keys, con):
        keys['date_position'] = keys['date_position'].astype('str')
        # keys['activate'] = keys['activate'].astype('str')
        # keys['english'] = keys['english'].astype('str')
        keys['remote'] = keys['remote'].astype('str')
        keys['salary'] = keys['salary'].astype('float64')
        
        df2 = df.merge(keys, how='left', on=[
            'position_title', 'position_category_id',
            'seniority_id', 'modality',
            'date_position', 'activate', 'num_offers',
            'salary_min', 'salary_max', 'salary',
            'currency_id', 'remote', 'location_id',
            'english', 'english_level', 'position_url',
            'company_id'])

        df2 = df2[['id_position', 'skill']]
        
        df2['skill'] = [x.replace('[', '').replace(']', '').replace(
            "'", '').strip().split(', ') for x in df2['skill']]

        positions = []
        skills = []
        # print(df2['skill'][0][0])

        for x in range(len(df2)):
            for skill in df2['skill'][x]:
                pos = df2['id_position'][x]
                positions.append(pos)
                skills.append(skill)
                # print(f'posicion: { pos} skill {skill}')

        df2 = pd.DataFrame({'position_id': positions, 'skill_id': skills})
        df2 = df2.drop_duplicates().dropna()

        registered = self.get_data('position_skill', con).drop(
            ['id_position_skill'], axis=1)
        # print(df.columns)
        df2['key'] = df2['position_id'].astype('str') + df2['skill_id'].astype('str')
        registered['key'] = registered['position_id'].astype('str') + registered['skill_id'].astype('str')
        news = df2[~df2['key'].isin(registered['key'])]
        news = news.drop(['key'], axis = 1)
        skl_id = list(skill_['id_skill'])
        skl_nm = list(skill_['skill'])

        for x in range(len(skl_id)):
            news['skill_id'] = news['skill_id'].replace(
                skl_nm[x], skl_id[x])
                
        news = news.fillna(1)

        if news.empty:
            print('No existen registros nuevos para position_skill')
        else:
            conn = con
            news.to_sql('position_skill', con=conn,
                        if_exists='append',
                        index=False)
            print(f'Se cargaron : {len(news)} registros')


if __name__ == '__main__':
    a = Load()

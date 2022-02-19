import argparse
import hashlib
import logging

from sqlalchemy.sql.expression import true
logging.basicConfig(level=logging.INFO)

import pandas as pd

from requests.exceptions import HTTPError
from urllib3.exceptions import MaxRetryError
from datetime import datetime

from position import Position
from company import Company
from currency import Currency
from position_category import PositionCategory
from location import Location
from base import Base, engine, Session

logger = logging.getLogger(__name__)

def main(filename):
    Base.metadata.create_all(engine)
    session = Session()
    job_positions = pd.read_csv(filename)
    job_positions = _change_column_str_to_datetime(job_positions, 'public_job_day')
    job_positions = _change_column_str_to_datetime(job_positions, 'execution_Day')
    job_positions = _add_some_columns_to_position(job_positions)

    _create_position_category_on_db(session, job_positions)
    job_positions = _get_id_from_table_position_category(session, job_positions)

    _create_location_on_db(session, job_positions)
    job_positions = _get_id_from_table_location(session, job_positions)

    #job_positions = _add_some_columns_to_company(job_positions)
    _create_company_on_db(session, job_positions)
    job_positions = _get_id_from_table_company(session, job_positions)

    currency = pd.read_csv('currency.csv')
    _create_currency_on_db(session, currency)
    job_positions = _add_id_currency_to_DF(job_positions)
    #publication_date = advertisements['diaPublicacion'].iloc[0]
    #_delete_data_by_date(session, publication_date)
    
    for index, row in job_positions.iterrows():
        try:
            q = session.query(Position).filter_by(uid=row['uid']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for position_url', exc_info=False)    
        if q == None:
            logger.info('Loading position {} into DB'.format(row['url_job']))
            position_job = Position(row['title_job'],
                                row['id_position_category'],
                                4,
                                row['job_description'],
                                row['Contract_Type'],
                                row['public_job_day'],
                                row['activate'],
                                row['num_offers'],
                                row['salary_min'],
                                row['salary_max'],
                                row['salary'],
                                row['id_currency'],
                                row['remote'],
                                row['id_location'],
                                1,
                                'Hablado',
                                row['url_job'],
                                row['IdCompany'],
                                row['uid'])

            session.add(position_job)

    session.commit()
    session.close()

def _get_id_from_table_location(session, df):
    idsLocation = []
    for index, row in df.iterrows():
        try:
            q = session.query(Location).filter_by(country=row['Where_is_Job']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for Location', exc_info=False)
        if q != None:
            logger.info('Get ID Location {} from DB'.format(row['Where_is_Job']))
            idsLocation.append(q.id_location)
    
    df['id_location'] = idsLocation
    return df

def _get_id_from_table_position_category(session, df):
    idsPositionCategory = []
    for index, row in df.iterrows():
        try:
            q = session.query(PositionCategory).filter_by(category=row['Category_Job']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for PositionCategory', exc_info=False)
        if q != None:
            logger.info('Get ID PositionCategory {} from DB'.format(row['Category_Job']))
            idsPositionCategory.append(q.id_position_category)
    
    df['id_position_category'] = idsPositionCategory
    return df


def _get_id_from_table_company(session, df):
    idsCompany = []
    for index, row in df.iterrows():
        try:
            q = session.query(Company).filter_by(name=row['company_job']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for company_name', exc_info=False)
        if q != None:
            logger.info('Get ID company {} from DB'.format(row['company_job']))
            idsCompany.append(q.id_company)
    
    df['IdCompany'] = idsCompany
    return df

def _create_position_category_on_db(session, df):
    df = df['Category_Job'].drop_duplicates(keep='first').to_frame()

    for index, row in df.iterrows():
        try:
            q = session.query(PositionCategory).filter_by(category=row['Category_Job']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for PositionCategory', exc_info=False)    
        if q == None:
            logger.info('Loading PositionCategory {} into DB'.format(row['Category_Job']))
            job_category = PositionCategory(row['Category_Job'])

            session.add(job_category)

    session.commit() 

def _create_location_on_db(session, df):
    df = df['Where_is_Job'].drop_duplicates(keep='first').to_frame()

    for index, row in df.iterrows():
        try:
            q = session.query(Location).filter_by(country=row['Where_is_Job']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for Location', exc_info=False)    
        if q == None:
            logger.info('Loading Location {} into DB'.format(row['Where_is_Job']))
            job_place = Location(row['Where_is_Job'],
                                        'unknown')

            session.add(job_place)

    session.commit()     

def _add_id_currency_to_DF(df):
    df['id_currency'] = 23
    return df

def _create_currency_on_db(session, df):
    for index, row in df.iterrows():
        try:
            q = session.query(Currency).filter_by(currency=row['currency']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for currency', exc_info=False)    
        if q == None:
            logger.info('Loading currency {} into DB'.format(row['currency']))
            currency = Currency(row['currency'],
                                row['country'])

            session.add(currency)

    session.commit()


def _create_company_on_db(session, df):
    df = df['company_job'].drop_duplicates(keep='first').to_frame()
    df = _add_some_columns_to_company(df)
    for index, row in df.iterrows():
        try:
            q = session.query(Company).filter_by(name=row['company_job']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for company_name', exc_info=False)    
        if q == None:
            logger.info('Loading company {} into DB'.format(row['company_job']))
            company = Company(row['company_job'],
                                    row['description'],
                                    row['company_premium'],
                                    row['company_size'],
                                    row['ceo'],
                                    row['avg_reputation'],
                                    row['total_ratings'],
                                    row['ceo_score'],
                                    row['website'],
                                    row['culture_score'],
                                    row['work_life_balance'],
                                    row['stress_level'])

            session.add(company)

    session.commit()

def _add_some_columns_to_company(df):
    df['description'] = ''
    df['company_premium'] = False
    df['company_size'] = 0
    df['ceo'] = ''
    df['avg_reputation'] = 0
    df['total_ratings'] = 0
    df['ceo_score'] = 0
    df['website'] = ''
    df['culture_score'] = 0
    df['work_life_balance'] = 0
    df['stress_level'] = 0

    return df

def _add_some_columns_to_position(df):
    df['activate'] = 1
    df['num_offers'] = 1
    df['salary_min'] = 0
    df['salary_max'] = 0
    df['salary'] = 0
    df['salary_type'] = 'fixed'
    df['bonus'] = 0
    df['remote'] = 1
    df['salary_frecuency'] = 1

    return df

    
def _change_column_str_to_datetime(df, columnName):
    #logger.info('Changing column, from str to datetime {} into df'.format(df[columnName][0]))
    df[columnName] = pd.to_datetime(df[columnName].astype('datetime64[ns]'))
    return df

def _delete_data_by_date(session, publication_date):
    logger.info('delete data from DataBase with publication_date equal to:  {}'.format(publication_date))
    qs = session.query(Position).filter_by(public_job_day = publication_date).all()
    if qs != None:
        for q in qs:
            session.delete(q)
        session.commit()    
   

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The file you want to load into the db',
                        type=str)

    args = parser.parse_args()

    main(args.filename)
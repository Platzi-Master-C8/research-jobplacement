import argparse
import hashlib
import logging
from re import T

from sqlalchemy.sql.expression import true
logging.basicConfig(level=logging.INFO)

import pandas as pd

from requests.exceptions import HTTPError
from urllib3.exceptions import MaxRetryError
from datetime import datetime

from company import Company
from perk import Perk
from company_perk import CompanyPerk
from base import Base, engine, Session

logger = logging.getLogger(__name__)

def main(filename):
    Base.metadata.create_all(engine)
    session = Session()
    perks = pd.read_csv(filename)

    #perks = perks['perk'].drop_duplicates(keep='first').to_frame()

    _create_perks_on_db(session, perks)
    perks = _get_id_from_table_perk(session, perks)

    _create_company_on_db(session, perks)
    perks = _get_id_from_table_company(session, perks)

    perks = _delete_blank_perks(perks)

    #job_positions = _add_some_columns_to_company(job_positions)
    #_create_company_on_db(session, job_positions)
    #job_positions = _get_id_from_table_company(session, job_positions)
    dropDuplicateRows = ['perk']
    perks = perks.drop_duplicates(subset=dropDuplicateRows, keep='last').reset_index(drop = True)
    
    for index, row in perks.iterrows():
        try:
            q = session.query(CompanyPerk).filter_by(company_id=row['IdCompany'], perk_id=row['IdPerk']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for company_id and perk_id', exc_info=False)    
        if q == None:
            logger.info('Loading CompanyPerk {} into DB'.format(row['perk']))
            company_perk = CompanyPerk(row['IdCompany'],
                                row['IdPerk'])

            session.add(company_perk)

    session.commit()
    session.close()

def _delete_blank_perks(df):
    #Now try to delete fields without perks  
    df_without_perk = df[df['perk'] == ""]
    if not df_without_perk.empty:
        df = df.drop(df_without_perk.index, axis=0)
        #remove own index with default index, in case remove rows
        df.reset_index(inplace = True, drop = True)
    #-------------------------------------------------------------
    return df

def _get_id_from_table_company(session, df):
    idsCompany = []
    for index, row in df.iterrows():
        try:
            q = session.query(Company).filter_by(name=row['company']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for company_name', exc_info=False)
        if q != None:
            logger.info('Get ID company {} from DB'.format(row['company']))
            idsCompany.append(q.id_company)
    
    df['IdCompany'] = idsCompany
    return df

def _create_perks_on_db(session, df):
    df = df['perk'].drop_duplicates(keep='first').to_frame()
    for index, row in df.iterrows():
        try:
            q = session.query(Perk).filter_by(perk=row['perk']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for Perk', exc_info=False)    
        if q == None:
            logger.info('Loading Perk {} into DB'.format(row['perk']))
            perk = Perk(row['perk'])

            session.add(perk)

    session.commit()

def _get_id_from_table_perk(session, df):
    idsPerk = []
    for index, row in df.iterrows():
        try:
            q = session.query(Perk).filter_by(perk=row['perk']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for perk_name', exc_info=False)
        if q != None:
            logger.info('Get ID perk {} from DB'.format(row['perk']))
            idsPerk.append(q.id_perk)
    
    df['IdPerk'] = idsPerk
    return df    
   
def _create_company_on_db(session, df):
    df = df['company'].drop_duplicates(keep='first').to_frame()
    df = _add_some_columns_to_company(df)
    for index, row in df.iterrows():
        try:
            q = session.query(Company).filter_by(name=row['company']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for company_name', exc_info=False)    
        if q == None:
            logger.info('Loading company {} into DB'.format(row['company']))
            company = Company(row['company'],
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

   

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The file you want to load into the db',
                        type=str)

    args = parser.parse_args()

    main(args.filename)
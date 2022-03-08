# Logging
import logging

# Pandas
import pandas as pd
from pandas import DataFrame

# Request
from requests.exceptions import HTTPError

# Urrlib
from urllib3.exceptions import MaxRetryError

# Data models
from company import Company
from perk import Perk
from company_perk import CompanyPerk
from base import Base, engine, Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(filename):
    """
    This function load perks from a csv file into the database

    :param filename: csv file with perks
    """
    Base.metadata.create_all(engine)
    session = Session()
    perks = pd.read_csv(filename)

    _create_perks_on_db(session, perks)
    perks = _get_id_from_table_perk(session, perks)

    _create_company_on_db(session, perks)
    perks = _get_id_from_table_company(session, perks)

    perks = _delete_blank_perks(perks)
    dropDuplicateRows = ['perk']
    perks = perks.drop_duplicates(subset=dropDuplicateRows, keep='last').reset_index(drop=True)

    # Add some columns to the dataframe
    for index, row in perks.iterrows():
        query = None
        try:
            query = session.query(CompanyPerk).filter_by(company_id=row['IdCompany'], perk_id=row['IdPerk']).first()

        except(HTTPError, MaxRetryError):
            logger.warning('Error while look for company_id and perk_id', exc_info=False)

        if query is None:
            logger.info('Loading CompanyPerk {} into DB'.format(row['perk']))
            company_perk = CompanyPerk(row['IdCompany'],
                                       row['IdPerk'])

            session.add(company_perk)

    session.commit()
    session.close()


def _delete_blank_perks(df: DataFrame) -> DataFrame:
    """
    This function delete the blank perks from the dataframe

    :param df: dataframe with perks
    """
    df_without_perk = df[df['perk'] == ""]
    if not df_without_perk.empty:
        df = df.drop(df_without_perk.index, axis=0)
        df.reset_index(inplace=True, drop=True)
    return df


def _get_id_from_table_company(session, df: DataFrame) -> DataFrame:
    """
    This function get the id of the company from the database

    :param session: session with the database
    :param df: dataframe with the company name
    :return: dataframe with the id of the company
    """
    idsCompany = []
    for index, row in df.iterrows():
        query = None
        try:
            query = session.query(Company).filter_by(name=row['company']).first()
        except(HTTPError, MaxRetryError):
            logger.warning('Error while look for company_name', exc_info=False)
        if query is not None:
            logger.info('Get ID company {} from DB'.format(row['company']))
            idsCompany.append(query.id_company)

    df['IdCompany'] = idsCompany
    return df


def _create_perks_on_db(session, df: DataFrame):
    """
    This function create the perks on the database

    :param session: session with the database
    :param df: dataframe with the perks
    """
    df = df['perk'].drop_duplicates(keep='first').to_frame()
    for index, row in df.iterrows():
        query = None
        try:
            query = session.query(Perk).filter_by(perk=row['perk']).first()
        except(HTTPError, MaxRetryError):
            logger.warning('Error while look for Perk', exc_info=False)
        if query is None:
            logger.info('Loading Perk {} into DB'.format(row['perk']))
            perk = Perk(row['perk'])

            session.add(perk)

    session.commit()


def _get_id_from_table_perk(session, df: DataFrame) -> DataFrame:
    """
    This function get the id of the perk from the database

    :param session: session with the database
    :param df: dataframe with the perks
    :return: dataframe with the id of the perk
    """
    idsPerk = []
    for index, row in df.iterrows():
        query = None
        try:
            query = session.query(Perk).filter_by(perk=row['perk']).first()
        except(HTTPError, MaxRetryError):
            logger.warning('Error while look for perk_name', exc_info=False)
        if query is not None:
            logger.info('Get ID perk {} from DB'.format(row['perk']))
            idsPerk.append(query.id_perk)

    df['IdPerk'] = idsPerk
    return df


def _create_company_on_db(session, df: DataFrame):
    """
    This function create the company on the database

    :param session: session with the database
    :param df: dataframe with the company
    """
    df = df['company'].drop_duplicates(keep='first').to_frame()
    df = _add_some_columns_to_company(df)
    for index, row in df.iterrows():
        query = None
        try:
            query = session.query(Company).filter_by(name=row['company']).first()
        except(HTTPError, MaxRetryError):
            logger.warning('Error while look for company_name', exc_info=False)
        if query is None:
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


def _add_some_columns_to_company(df: DataFrame) -> DataFrame:
    """
    This function add some columns to the dataframe with default values

    :param df: dataframe with the company
    :return: dataframe with the company
    """
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

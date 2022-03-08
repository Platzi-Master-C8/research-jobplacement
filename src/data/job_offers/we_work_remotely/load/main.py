# Logging
import argparse
import logging

# Pandas
import pandas as pd
from pandas import DataFrame

# Request
from requests.exceptions import HTTPError

# Urrlib
from urllib3.exceptions import MaxRetryError

# Data Models
from position import Position
from company import Company
from currency import Currency
from position_category import PositionCategory
from location import Location
from base import Base, engine, Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(filename: str):
    """
    This function read the csv file and load it into the database

    :param filename: the name of the file to load
    """
    Base.metadata.create_all(engine)
    session = Session()
    job_positions = pd.read_csv(filename)
    job_positions = _change_column_str_to_datetime(job_positions, 'public_job_day')
    job_positions = _change_column_str_to_datetime(job_positions, 'execution_Day')
    job_positions = _add_some_columns_to_position(job_positions)

    # Create PositionCategory on DB
    _create_position_category_on_db(session, job_positions)
    job_positions = _get_id_from_table_position_category(session, job_positions)

    # Create Location on DB
    _create_location_on_db(session, job_positions)
    job_positions = _get_id_from_table_location(session, job_positions)

    # Create Company on DB
    _create_company_on_db(session, job_positions)
    job_positions = _get_id_from_table_company(session, job_positions)

    # Create Currency on DB
    currency = pd.read_csv('currency.csv')
    _create_currency_on_db(session, currency)
    job_positions = _add_id_currency_to_DF(job_positions)

    for index, row in job_positions.iterrows():
        query = None
        try:
            query = session.query(Position).filter_by(uid=row['uid']).first()
        except(HTTPError, MaxRetryError):
            logger.warning('Error while look for position_url', exc_info=False)
        if query is None:
            logger.info(f'Loading position {row["url_job"]} into DB')
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


def _get_id_from_table_location(session, df: DataFrame) -> DataFrame:
    """
    This function get the id from the table location

    :param session: the session to the database
    :param df: the dataframe to add the id to
    :return: the dataframe with the id
    """
    idsLocation = []
    for index, row in df.iterrows():
        query = None
        try:
            query = session.query(Location).filter_by(country=row['Where_is_Job']).first()
        except(HTTPError, MaxRetryError):
            logger.warning('Error while look for Location', exc_info=False)
        if query is not None:
            logger.info(f'Get ID Location {row["Where_is_Job"]} from DB')
            idsLocation.append(query.id_location)

    df['id_location'] = idsLocation
    return df


def _get_id_from_table_position_category(session, df: DataFrame) -> DataFrame:
    """
    This function get the id from the table position_category

    :param session: the session to the database
    :param df: the dataframe to add the id to
    :return: the dataframe with the id
    """
    idsPositionCategory = []
    for index, row in df.iterrows():
        query = None
        try:
            query = session.query(PositionCategory).filter_by(category=row['Category_Job']).first()
        except(HTTPError, MaxRetryError):
            logger.warning('Error while look for PositionCategory', exc_info=False)
        if query is not None:
            logger.info(f'Get ID PositionCategory {row["Category_Job"]} from DB')
            idsPositionCategory.append(query.id_position_category)

    df['id_position_category'] = idsPositionCategory
    return df


def _get_id_from_table_company(session, df: DataFrame) -> DataFrame:
    """
    This function get the id from the table company

    :param session: the session to the database
    :param df: the dataframe to add the id to
    :return: the dataframe with the id
    """
    idsCompany = []
    for index, row in df.iterrows():
        query = None
        try:
            query = session.query(Company).filter_by(name=row['company_job']).first()
        except(HTTPError, MaxRetryError):
            logger.warning('Error while look for company_name', exc_info=False)
        if query is not None:
            logger.info(f'Get ID company {row["company_job"]} from DB')
            idsCompany.append(query.id_company)

    df['IdCompany'] = idsCompany
    return df


def _create_position_category_on_db(session, df: DataFrame) -> None:
    """
    This function create the position_category on the database

    :param session: the session to the database
    :param df: the dataframe to add the id to
    """
    df = df['Category_Job'].drop_duplicates(keep='first').to_frame()

    for index, row in df.iterrows():
        query = None
        try:
            query = session.query(PositionCategory).filter_by(category=row['Category_Job']).first()
        except(HTTPError, MaxRetryError):
            logger.warning('Error while look for PositionCategory', exc_info=False)
        if query is None:
            logger.info(f'Loading PositionCategory {row["Category_Job"]} into DB')
            job_category = PositionCategory(row['Category_Job'])

            session.add(job_category)

    session.commit()


def _create_location_on_db(session, df: DataFrame) -> None:
    """
    This function create the location on the database

    :param session: the session to the database
    :param df: the dataframe to add the id to
    """
    df = df['Where_is_Job'].drop_duplicates(keep='first').to_frame()

    for index, row in df.iterrows():
        query = None
        try:
            query = session.query(Location).filter_by(country=row['Where_is_Job']).first()
        except(HTTPError, MaxRetryError):
            logger.warning('Error while look for Location', exc_info=False)
        if query is None:
            logger.info(f'Loading Location {row["Where_is_Job"]} into DB')
            job_place = Location(row['Where_is_Job'], 'unknown')

            session.add(job_place)

    session.commit()


def _add_id_currency_to_DF(df: DataFrame) -> DataFrame:
    """
    This function add the id_currency to the dataframe by default the id_currency is 23 (USD)

    :param df: the dataframe to add the id to
    :return: the dataframe with the id
    """
    df['id_currency'] = 23
    return df


def _create_currency_on_db(session, df: DataFrame) -> None:
    """
    This function create the currency on the database

    :param session: the session to the database
    :param df: the dataframe to add the id to
    """
    for index, row in df.iterrows():
        query = None
        try:
            query = session.query(Currency).filter_by(currency=row['currency']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for currency', exc_info=False)
        if query is None:
            logger.info(f'Loading currency {row["currency"]} into DB')
            currency = Currency(row['currency'],
                                row['country'])

            session.add(currency)

    session.commit()


def _create_company_on_db(session, df: DataFrame):
    """
    This function create the company on the database

    :param session: the session to the database
    :param df: the dataframe to add the id to
    """
    df = df['company_job'].drop_duplicates(keep='first').to_frame()
    df = _add_some_columns_to_company(df)
    for index, row in df.iterrows():
        query = None
        try:
            query = session.query(Company).filter_by(name=row['company_job']).first()
        except(HTTPError, MaxRetryError) as e:
            logger.warning('Error while look for company_name', exc_info=False)
        if query is None:
            logger.info(f'Loading company {row["company_job"]} into DB')
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


def _add_some_columns_to_company(df: DataFrame) -> DataFrame:
    """
    This function add some columns to the dataframe with default values

    :param df: the dataframe to add the columns to
    :return: the dataframe with the columns
    """
    df['description'] = 'None'
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


def _add_some_columns_to_position(df: DataFrame) -> DataFrame:
    """
    This function add some columns to the dataframe with default values

    :param df: the dataframe to add the columns to
    :return: the dataframe with the columns
    """
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


def _change_column_str_to_datetime(df: DataFrame, columnName: str) -> DataFrame:
    """
    This function change the column to datetime

    :param df: the dataframe to change the column
    :param columnName: the name of the column to change
    :return: the dataframe with the column changed
    """
    df[columnName] = pd.to_datetime(df[columnName].astype('datetime64[ns]'))
    return df


def _delete_data_by_date(session, publication_date: str) -> None:
    """
    This function delete the data by date from the database

    :param session: the session to the database
    :param publication_date: the date to delete the data
    """
    logger.info('delete data from DataBase with publication_date equal to:  {}'.format(publication_date))
    query_with_filter = session.query(Position).filter_by(public_job_day=publication_date).all()
    if query_with_filter is not None:
        for result in query_with_filter:
            session.delete(result)
        session.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The file you want to load into the db',
                        type=str)

    args = parser.parse_args()

    main(args.filename)

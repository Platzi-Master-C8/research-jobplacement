# Python
import argparse
import re
import datetime
import hashlib

# Logging
import logging

# Pandas
import pandas as pd
from pandas import DataFrame

# BeautifulSoup
from bs4 import BeautifulSoup

# Constants
currentDT = datetime.datetime.now()
currentDateTime = f'{currentDT.year}_{currentDT.month}_{currentDT.day}'
perksFileName = f'perks_{currentDateTime}_company.csv'

# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(filename: str) -> DataFrame:
    """
    Main function to be executed by the script to process the data

    :param filename: str - filename to be processed
    :return: DataFrame - DataFrame with the processed data
    """
    logger.info('Starting cleaning process')

    df = _read_data(filename)
    dfPerks = _lookPerksIntoJobDescription(df)
    _save_data_updated(perksFileName, dfPerks)
    df = _add_execution_date_column(df)
    df = _delete_tagHtml_from_columns(df)
    df = _add_new_columns_from_one_column(df)
    df = _delete_column('jobTitle_Category_JobPlace', df)
    df = _delete_white_space(df)
    df = _add_Column_id(df)
    df = _remove_duplicate_entries(df, 'url_job')
    df = _drop_rows_with_missing_data(df)

    cleanFileName = 'clean_' + filename
    _save_data_updated(cleanFileName, df)

    return df


def _remove_duplicate_entries(df: DataFrame, column_name: str) -> DataFrame:
    """
    Remove duplicate entries from a dataframe based on a column name

    :param df: DataFrame  - DataFrame to be cleaned
    :param column_name: str - column name to be used to remove duplicates
    """
    logger.info('Removing duplicate entries')
    df.drop_duplicates(subset=[column_name], keep='first', inplace=True)
    return df


def _drop_rows_with_missing_data(df: DataFrame) -> DataFrame:
    """
    Drop rows with missing data from a dataframe

    :param df: DataFrame - DataFrame to be cleaned
    :return: DataFrame - DataFrame without the rows with missing data
    """
    logger.info('Dropping rows with missing values')
    return df.dropna()


def _add_Column_id(df: DataFrame) -> DataFrame:
    """
    Add a column with the id of the job offer based on the url

    :param df: DataFrame - DataFrame to be cleaned
    :return: DataFrame - DataFrame with the new column
    """
    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url_job'].encode())), axis=1)
            .apply(lambda hash_object: hash_object.hexdigest())
            )
    df['uid'] = uids
    df.set_index('uid', inplace=True)
    return df


def _delete_column(nameColumnToDelete: str, df: DataFrame) -> DataFrame:
    """
    Delete a column from a dataframe based on the name of the column

    :param nameColumnToDelete: str - name of the column to be deleted
    :param df: DataFrame - DataFrame to be cleaned
    :return: DataFrame - DataFrame without the column
    """
    df = df.drop(nameColumnToDelete, 1)
    return df


def _add_new_columns_from_one_column(df: DataFrame) -> DataFrame:
    """
    Add new columns from one column of the dataframe

    :param df: DataFrame - DataFrame to be cleaned
    :return: DataFrame - DataFrame with the new columns
    """
    df[['Contract_Type', 'Category_Job', 'Where_is_Job']] = df['jobTitle_Category_JobPlace'].str.split(',', n=2,
                                                                                                       expand=True)
    df['Contract_Type'] = df['Contract_Type'].str.replace('[', '')
    df['job_description'] = df['job_description'].str.replace('[', '')
    df['job_description'] = df['job_description'].str.replace(']', '')
    df['Where_is_Job'] = df['Where_is_Job'].str.replace(']', '')

    return df


def _delete_tagHtml_from_columns(df: DataFrame) -> DataFrame:
    """
    Delete the html tags from the columns of the dataframe

    :param df: DataFrame - DataFrame to be cleaned
    :return: DataFrame - DataFrame without the html tags
    """
    df['jobTitle_Category_JobPlace'] = df['jobTitle_Category_JobPlace'].str.replace(r'<!--.*?-->|<[^>]*>', '',
                                                                                    regex=True)
    df['job_description'] = df['job_description'].str.replace(r'<!--.*?-->|<[^>]*>', ' ', regex=True)
    return df


def _delete_white_space(df: DataFrame) -> DataFrame:
    """
    Delete the white space from the columns of the dataframe

    :param df: DataFrame - DataFrame to be cleaned
    :return: DataFrame - DataFrame without the white space
    """
    df['company_job'] = df['company_job'].str.rstrip()
    df['job_description'] = df['job_description'].str.rstrip()
    df['public_job_day'] = df['public_job_day'].str.strip()
    df['title_job'] = df['title_job'].str.rstrip()
    df['url_job'] = df['url_job'].str.rstrip()
    df['Contract_Type'] = df['Contract_Type'].str.rstrip()
    df['Category_Job'] = df['Category_Job'].str.rstrip()
    df['Where_is_Job'] = df['Where_is_Job'].str.rstrip()

    # Now try to change fields without company_job by label "Without Company"
    df_without_company_job = df[df['company_job'] == ""]
    if not df_without_company_job.empty:
        df.at[df_without_company_job.index, 'company_job'] = "Without Company"

    # Now try to change fields without job description by label "NULL"
    df_without_job_description = df[df['job_description'] == ""]
    if not df_without_job_description.empty:
        df.at[df_without_job_description.index, 'job_description'] = "NULL"

    # Now try to delete fields without company
    df_without_company = df[df['company_job'] == ""]
    if not df_without_company.empty:
        df = df.drop(df_without_company.index, axis=0)
        # remove own index with default index, in case remove rows
        df.reset_index(inplace=True, drop=True)

    return df


def _save_data_updated(filename: str, df: DataFrame) -> None:
    """
    Save the dataframe in a csv file with the name filename

    :param filename: str - name of the file to be saved
    :param df: DataFrame - DataFrame to be saved
    """
    df.to_csv(filename)


def _read_data(filename: str) -> DataFrame:
    """
    Read the dataframe from a csv file with the name filename and return it

    :param filename: str - name of the file to be read
    :return: DataFrame - DataFrame read
    """
    logger.info('Reading file {}'.format(filename))

    return pd.read_csv(filename, sep=',', engine='python')


def _extract_execution_date(filename: str) -> str:
    """
    Extract the execution date from the filename of the dataframe

    :param filename: str - name of the file to be read
    :return: str - execution date
    """
    logging.info('Extracting publication date')
    pub_year = filename.split('_')[1]
    pub_month = filename.split('_')[2]
    pub_day = filename.split('_')[3]
    execution_date = f'{pub_year}/{pub_month}/{pub_day}'

    logger.info('Execution Date Detected: {}'.format(execution_date))
    return execution_date


def _add_execution_date_column(df):
    """
    Add a column with the execution date to the dataframe

    :param df: DataFrame - DataFrame to be cleaned
    :return: DataFrame - DataFrame with the execution date
    """
    DT = datetime.datetime.now()
    logger.info('Filling execution_date column with {}'.format(DT))
    pub_year = DT.year
    pub_month = DT.month
    pub_day = DT.day
    execution_date = f'{pub_year}/{pub_month}/{pub_day}'

    df['execution_Day'] = execution_date

    return df


def _lookPerksIntoJobDescription(df: DataFrame) -> DataFrame:
    """
    Look for perks in the job description and add them to the dataframe

    :param df: DataFrame - DataFrame to be cleaned
    :return: DataFrame - DataFrame with the perks
    """
    position = 0
    companies = []
    perks = []
    while position < len(df):
        textAfterPerks = lookTitlePerksInDescriptionJobBody(getTitlePerks(), df.iloc[position][2])
        if textAfterPerks != '':
            iUL = textAfterPerks.find('<ul>')
            iDIV = textAfterPerks.find('<div>')
            iStrong = textAfterPerks.find('<strong>')
            toLook = whichIsMinor(iUL, iDIV, iStrong)
            soup = BeautifulSoup(textAfterPerks, 'html.parser')

            if toLook == 1:
                listaPerks = soup.select('ul')
                if len(listaPerks) > 0:
                    listFinal = listaPerks[0].find_all("li")
                    for perk in listFinal:
                        perk = str(perk)
                        perk = re.sub(r'<!--.*?-->|<[^>]*>', r'', perk)
                        companies.append(df.iloc[position][0])
                        perks.append(perk)

            elif toLook == 2:
                listaPerks = soup.select('div')
                if len(listaPerks) > 0:
                    for perk in listaPerks:
                        perk = str(perk)
                        idBr = perk.find('<br/>')
                        if idBr == -1:
                            perk = re.sub(r'<!--.*?-->|<[^>]*>', r'', perk)
                            companies.append(df.iloc[position][0])
                            perks.append(perk)
                        else:
                            break
            elif toLook == 3:
                listaPerks = soup.select('strong')
                if len(listaPerks) > 0:
                    for perk in listaPerks:
                        perk = str(perk)
                        idBr = perk.find('<br/>')
                        if idBr == -1:
                            perk = re.sub(r'<!--.*?-->|<[^>]*>', r'', perk)
                            companies.append(df.iloc[position][0])
                            perks.append(perk)
                        else:
                            break

        position += 1
    list_tuples = list(zip(companies, perks))
    perkDFrame = pd.DataFrame(list_tuples, columns=['company', 'perk'])
    perkDFrame['company'] = perkDFrame['company'].str.rstrip()
    return perkDFrame


def lookTitlePerksInDescriptionJobBody(perkList: list, textBody: str) -> str:
    """
    Look for perks in the job description and add them to the dataframe

    :param perkList: list - list of perks
    :param textBody: str - text to be looked
    """
    textAfterPerks = ''
    for perk in perkList:
        idPerks = textBody.find(perk)
        if idPerks != -1:
            textAfterPerks = textBody[idPerks:]
            break
    return textAfterPerks


def getTitlePerks() -> list:
    """
    Get the list of perks to look for in the job description

    :return: list - list of perks
    """
    perkList = ["Why you'll like working here", 'Benefits',
                "The top five reasons our team members joined us (according to them)",
                "What’s in it for you?", "Some benefits", "Here’s what you’ll get if you join", "What you get",
                "What We Offer", "benefits and perks",
                "Why should I choose", "What we offer", "We offer", "Our Perks", "What You’ll Get", "Perks",
                "What we offe"]
    return perkList


def whichIsMinor(ul: int, div: int, strong: int) -> int:
    """
    Which tag is minor? (ul, div, strong)

    :param ul: int - position of ul
    :param div: int - position of div
    :param strong: int - position of strong
    :return: int - position of the minor tag
    """
    if ul > 0 and div > 0 and strong > 0:
        if ul < div and ul < strong:
            return 1
        elif div < ul and div < strong:
            return 2
        elif strong < ul and strong < div:
            return 3
    elif ul == -1 and div > 0 and strong > 0:
        if div < strong:
            return 2
        else:
            return 3
    elif ul > 0 and div == -1 and strong > 0:
        if ul < strong:
            return 1
        else:
            return 3
    elif ul > 0 and div > 0 and strong == -1:
        if ul < div:
            return 1
        else:
            return 2
    elif ul > 0 and div == -1 and strong == -1:
        return 1
    elif ul == -1 and div > 0 and strong == -1:
        return 2
    elif ul == -1 and div == -1 and strong > 0:
        return 3


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The path to the dirty data',
                        type=str)

    args = parser.parse_args()

    df = main(args.filename)
    print(df)

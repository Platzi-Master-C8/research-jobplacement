import argparse
import logging
import datetime
import re
from readline import get_current_history_length
from bs4 import BeautifulSoup 
import numpy
import hashlib
logging.basicConfig(level=logging.INFO)
from urllib.parse import urlparse

import pandas as pd

currentDT = datetime.datetime.now()
currentDateTime = str(currentDT.year) + "_" + str(currentDT.month) + "_" + str(currentDT.day)
perksFileName = 'perks' + "_" + currentDateTime + "_company.csv"

logger = logging.getLogger(__name__)

def main(filename):
    logger.info('Starting cleaning process')

    df = _read_data(filename)
    #execution_date = _extract_execution_date(filename)
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

def _remove_duplicate_entries(df, column_name):
    logger.info('Removing duplicate entries')
    df.drop_duplicates(subset=[column_name], keep='first', inplace=True)
    return df

def _drop_rows_with_missing_data(df):
    logger.info('Dropping rows with missing values')
    return df.dropna()

def _add_Column_id(df):
    uids = (df
        .apply(lambda row : hashlib.md5(bytes(row['url_job'].encode())), axis=1)
        .apply(lambda hash_object : hash_object.hexdigest())
        )
    df['uid'] = uids
    df.set_index('uid', inplace = True)
    return df

def _delete_column(nameColumnToDelete, df):
    df = df.drop(nameColumnToDelete, 1)
    return df

def _add_new_columns_from_one_column(df):
    df[['Contract_Type','Category_Job','Where_is_Job']] = df['jobTitle_Category_JobPlace'].str.split(',', n=2, expand=True)
    df['Contract_Type'] = df['Contract_Type'].str.replace('[','')
    df['job_description'] = df['job_description'].str.replace('[','')
    df['job_description'] = df['job_description'].str.replace(']','')
    df['Where_is_Job'] = df['Where_is_Job'].str.replace(']','')

    return df

def _delete_tagHtml_from_columns(df):
    df['jobTitle_Category_JobPlace'] = df['jobTitle_Category_JobPlace'].str.replace(r'<!--.*?-->|<[^>]*>', '', regex=True)
    df['job_description'] = df['job_description'].str.replace(r'<!--.*?-->|<[^>]*>', ' ', regex=True)
    return df

def _delete_white_space(df):
    df['company_job'] = df['company_job'].str.rstrip()
    df['job_description'] = df['job_description'].str.rstrip()
    df['public_job_day'] = df['public_job_day'].str.strip()
    df['title_job'] = df['title_job'].str.rstrip()
    df['url_job'] = df['url_job'].str.rstrip()
    df['Contract_Type'] = df['Contract_Type'].str.rstrip()
    df['Category_Job'] = df['Category_Job'].str.rstrip()
    df['Where_is_Job'] = df['Where_is_Job'].str.rstrip()


    #Now try to change fields without company_job by label "Without Company"
    df_without_company_job = df[df['company_job'] == ""]
    if not df_without_company_job.empty:
        df.at[df_without_company_job.index, 'company_job'] = "Without Company"
    #-------------------------------------------------------------
    #Now try to change fields without job description by label "NULL"
    df_without_job_description = df[df['job_description'] == ""]
    if not df_without_job_description.empty:
        df.at[df_without_job_description.index, 'job_description'] = "NULL"
    #-------------------------------------------------------------
    #Now try to delete fields without company  
    df_without_company = df[df['company_job'] == ""]
    if not df_without_company.empty:
        df = df.drop(df_without_company.index, axis=0)
        #remove own index with default index, in case remove rows
        df.reset_index(inplace = True, drop = True)
    #-------------------------------------------------------------
    return df


def _save_data_updated(filename, df):
    #filename = filename.split('.')[0]
    #clean_filename = 'clean_{}'.format(filename)
    #df.to_csv(clean_filename + '_' + str(datetime.datetime.now().hour) + '_' + str(datetime.datetime.now().minute) + '.csv')
    df.to_csv(filename)


def _read_data(filename):
    logger.info('Reading file {}'.format(filename))

    return pd.read_csv(filename, sep=',', engine='python')

def _extract_execution_date(filename):
    logging.info('Extracting publication date')
    pub_year = filename.split('_')[1]
    pub_month = filename.split('_')[2]
    pub_day = filename.split('_')[3]
    execution_date = pub_year + '/' + pub_month + '/' + pub_day

    logger.info('Execution Date Detected: {}'.format(execution_date))
    return execution_date


def _add_execution_date_column(df):
    DT = datetime.datetime.now()
    logger.info('Filling execution_date column with {}'.format(DT))
    pub_year = DT.year
    pub_month = DT.month
    pub_day = DT.day
    execution_date = str(pub_year) + '/' + str(pub_month) + '/' + str(pub_day)

    df['execution_Day'] = execution_date

    return df

def _lookPerksIntoJobDescription(df):
    i = 0
    companies = []
    perks = []
    while i < len(df):
        textAfterPerks = ''
        textAfterPerks = lookTitlePerksInDescriptionJobBody(getTitlePerks(), df.iloc[i][2])
        if textAfterPerks != '':
            iUL = 0
            iDIV = 0
            iStrong = 0
            iUL = textAfterPerks.find('<ul>')
            iDIV = textAfterPerks.find('<div>')
            iStrong = textAfterPerks.find('<strong>')
            toLook = whichIsMinor(iUL,iDIV,iStrong)
            soup = BeautifulSoup(textAfterPerks, 'html.parser')
            
            if (toLook == 1):
                listaPerks = soup.select('ul')
                if len(listaPerks) > 0:
                    listFinal = listaPerks[0].find_all("li")
                    for perk in listFinal:
                        perk = str(perk)
                        perk = re.sub(r'<!--.*?-->|<[^>]*>',r'', perk)
                        companies.append(df.iloc[i][0])
                        perks.append(perk)
            
            elif (toLook == 2):
                listaPerks = soup.select('div')
                if len(listaPerks) > 0:
                    for perk in listaPerks:
                        perk = str(perk)
                        idBr = perk.find('<br/>')
                        if idBr == -1:
                            perk = re.sub(r'<!--.*?-->|<[^>]*>',r'', perk)
                            companies.append(df.iloc[i][0])
                            perks.append(perk)
                        else:
                            break
            
            elif (toLook == 3):
                listaPerks = soup.select('strong')
                if len(listaPerks) > 0:
                    for perk in listaPerks:
                        perk = str(perk)
                        idBr = perk.find('<br/>')
                        if idBr == -1:
                            perk = re.sub(r'<!--.*?-->|<[^>]*>',r'', perk)
                            companies.append(df.iloc[i][0])
                            perks.append(perk)
                        else:
                            break
                
        i = i + 1
    #  two lists.  
    # and merge them by using zip().  
    list_tuples = list(zip(companies, perks))
    # Converting lists of tuples into  
    # pandas Dataframe.  
    perkDFrame = pd.DataFrame(list_tuples, columns=['company', 'perk'])
    perkDFrame['company'] = perkDFrame['company'].str.rstrip()
    return perkDFrame


def lookTitlePerksInDescriptionJobBody(perkList, textBody):
    textAfterPerks = ''
    for perk in perkList:
        idPerks = textBody.find(perk)
        if idPerks != -1:
            textAfterPerks = textBody[idPerks:]
            break
    return textAfterPerks


def getTitlePerks():
    perkList = ["Why you'll like working here",'Benefits', "The top five reasons our team members joined us (according to them)",
             "What’s in it for you?", "Some benefits", "Here’s what you’ll get if you join","What you get", "What We Offer", "benefits and perks",
             "Why should I choose","What we offer","We offer", "Our Perks", "What You’ll Get", "Perks", "What we offe"]
    return perkList

def whichIsMinor(ul, div, strong):
    if (ul > 0 and div > 0 and strong > 0):
        if(ul < div and ul < strong):
            return 1
        elif (div < ul and div < strong):
            return 2
        elif (strong < ul and strong < div):
            return 3
    elif (ul == -1 and div > 0 and strong > 0):
        if (div < strong):
            return 2
        else:
            return 3
    elif (ul > 0 and div == -1 and strong > 0):
        if (ul < strong):
            return 1
        else:
            return 3
    elif (ul > 0 and div > 0 and strong == -1):
        if (ul < div):
            return 1
        else:
            return 2
    elif (ul > 0 and div == -1 and strong == -1):
        return 1
    elif (ul == -1 and div > 0 and strong == -1):
        return 2
    elif (ul == -1 and div == -1 and strong > 0):
        return 3


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The path to the dirty data',
                        type=str)

    args = parser.parse_args()

    df = main(args.filename)
    print(df)
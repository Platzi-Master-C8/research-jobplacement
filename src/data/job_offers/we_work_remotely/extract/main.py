import argparse
import logging
import pandas as pd
import datetime
import csv
logging.basicConfig(level=logging.INFO)
import re

from requests.exceptions import HTTPError
from urllib3.exceptions import MaxRetryError

import job_page_objects as jobs
from common import config

logger = logging.getLogger(__name__)
is_well_formed_link = re.compile(r'^https?://.+/.+$') #Example https://example.com/hello
is_root_path = re.compile(r'^/.+$') # /some-text
#is_str_with_space = re.compile(r'^.+')

def _job_scraper(job_site_uid):
    host = config()['job_sites'][job_site_uid]['url']
    logging.info('Beginning scraper for {}'.format(job_site_uid))
    homepage = jobs.HomePage(job_site_uid, host)

    categoryLinks = []
    i = 0
    #Here, I get categories link from home page, and I go through them.
    logging.info('Beginning scraper for link JOBS')
    jobs_detail = []
    list_jobs = []
    for link in homepage.jobCategories_links:
        #print(link)
        aux = _build_link(host, link)
        #print(aux)
        list_jobs = _fetch_job_by_category(job_site_uid, host, link)
        for job_link in list_jobs.jobOffer_links:
            #print('aqui job_link {}'.format(job_link))
            job = _fetch_job(job_site_uid, host, job_link)
            #print(type(job))
            #print(dir(job))
            if job:
                logger.info('Job fetched!')
                jobs_detail.append(job)
                
    _save_jobs(job_site_uid, jobs_detail)
    

def _build_link(host, link):
    if is_well_formed_link.match(link):
        return link
    elif is_root_path.match(link):
        return '{}{}'.format(host, link)
    else:
        return '{host}/{uri}'.format(host=host, uri=link)


def _create_csv_file(news_site_uid):
    #now = datetime.datetime.now().strftime('%Y_%m_%d')
    currentDT = datetime.datetime.now()
    #fechaPublicacion = ('%(ano)s_%(mes)s_%(dia)s' % {'ano': currentDT.year, 'mes':currentDT.month,'dia':currentDT.day}) 
    out_file_name = '{news_site_uid}_{datetime}_jobs.csv'.format(
        news_site_uid = news_site_uid,
        datetime = currentDT)

    csv_headers = ['urlLinkJob', 'titleJob', 'companyJob', 'publicJobDay', 'properties', 'descriptionJob']
    #csv_headers = list(dir(propertiesByAds[0]))

    with open(out_file_name, mode='w+') as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)

    return out_file_name


def _save_jobs(job_site_uid, jobs_detail):
    currentDT = datetime.datetime.now()
    #out_file_name = '{news_site_uid}_{datetime}_jobs.csv'.format(
    #    news_site_uid = job_site_uid,
    #    datetime = currentDT)

    currentDateTime = str(currentDT.year) + "_" + str(currentDT.month) + "_" + str(currentDT.day)
    fileNameWithRawData = job_site_uid + "_" + currentDateTime + "_jobs.csv"

    csv_headers = list(filter(lambda property: not property.startswith('_'), dir(jobs_detail[0])))

    with open(fileNameWithRawData, mode='w+') as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)    

        for job in jobs_detail:
            row = [str(getattr(job, prop)) for prop in csv_headers]
            writer.writerow(row)  


def _fetch_job(job_site_uid, host, link):
    logger.info('Start fetching job at :  {}'.format(link))
    job = None
    try:
        job = jobs.Offer_job_Page(job_site_uid, _build_link(host, link))
    except(HTTPError, MaxRetryError) as e:
        logger.warning('Error while fechting JOB', exc_info=False)

    if job and not job.url_job:
        logger.warning('Invalid url_job. There is not Job URL')
        return None

    return job

def _fetch_job_by_category(job_site_uid, host, link):
    logger.info('Start fetching job at follow category:  {}'.format(link))
    category = None
    try:
        category = jobs.Offer_jobs_by_Category_Page(job_site_uid, _build_link(host, link))
    except(HTTPError, MaxRetryError) as e:
        logger.warning('Error while fechting JOBs', exc_info=False)

    if category and not category.jobOffer_links:
        logger.warning('Invalid list_job. There is not Job Offer Links')
        return None

    return category

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    job_site_choices = list(config()['job_sites'].keys())
    parser.add_argument('job_site',
                        help='The Job site that you want to scrape',
                        type=str,
                        choices=job_site_choices) 

    args = parser.parse_args()
    _job_scraper(args.job_site)
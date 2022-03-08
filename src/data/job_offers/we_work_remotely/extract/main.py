# Python
import argparse
import datetime
import csv
import re

# Logging
import logging

# Job offers
import job_page_objects as jobs

# Request
from requests.exceptions import HTTPError

# Urllib
from urllib3.exceptions import MaxRetryError

# Config
from common import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
is_well_formed_link = re.compile(r'^https?://.+/.+$')  # Example https://example.com/hello
is_root_path = re.compile(r'^/.+$')  # Example /some-text


def _job_scraper(job_site_uid: str):
    """
    Scrapes jobs from the job_site_uid

    :param job_site_uid: The unique identifier for a job site
    """
    host = config()['job_sites'][job_site_uid]['url']
    logging.info('Beginning scraper for {}'.format(job_site_uid))
    homepage = jobs.HomePage(job_site_uid, host)

    # Get categories link from home page, and go through them.
    logging.info('Beginning scraper for link JOBS')
    jobs_detail = []
    for link in homepage.jobCategories_links:
        _build_link(host, link)
        list_jobs = _fetch_job_by_category(job_site_uid, host, link)
        for job_link in list_jobs.jobOffer_links:
            job = _fetch_job(job_site_uid, host, job_link)
            if job:
                logger.info('Job fetched!')
                jobs_detail.append(job)
    _save_jobs(job_site_uid, jobs_detail)


def _build_link(host: str, link: str):
    """
    Builds absolute url from relative url

    :param host: The website host
    :param link: The relative url
    :return: The absolute url
    """
    if is_well_formed_link.match(link):
        return link
    elif is_root_path.match(link):
        return f'{host}{link}'
    else:
        return f'{host}/{link}'


def _create_csv_file(news_site_uid: str):
    """
    Creates a new csv file for the news site uid

    :param news_site_uid: The unique identifier for a news site
    """
    currentDT = datetime.datetime.now()
    out_file_name = '{news_site_uid}_{datetime}_jobs.csv'.format(
        news_site_uid=news_site_uid,
        datetime=currentDT)

    csv_headers = ['urlLinkJob', 'titleJob', 'companyJob', 'publicJobDay', 'properties', 'descriptionJob']

    # Open a new csv file
    with open(out_file_name, mode='w+') as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)

    return out_file_name


def _save_jobs(job_site_uid: str, jobs_detail: list):
    """
    Saves jobs to a csv file for the job_site_uid

    :param job_site_uid: The unique identifier for a job site
    :param jobs_detail: The list of details of the jobs
    """
    currentDT = datetime.datetime.now()
    currentDateTime = f'{currentDT.year}_{currentDT.month}_{currentDT.day}'
    fileNameWithRawData = f'{job_site_uid}_{currentDateTime}_jobs.csv'

    csv_headers = list(filter(lambda header: not header.startswith('_'), dir(jobs_detail[0])))

    with open(fileNameWithRawData, mode='w+', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)

        for job in jobs_detail:
            row = [str(getattr(job, prop)) for prop in csv_headers]
            writer.writerow(row)


def _fetch_job(job_site_uid: str, host: str, link: str):
    """
    Fetches a job from the job_site_uid and link provided

    :param job_site_uid: The unique identifier for a job site
    :param host: The website host
    :param link: The relative url
    :return: The fetch job
    """
    logger.info('Start fetching job at :  {}'.format(link))
    job = None
    # Try to fetch the job offer page and parse it. If it fails, return None.
    try:
        job = jobs.OfferJobPage(job_site_uid, _build_link(host, link))
    except(HTTPError, MaxRetryError):
        logger.warning('Error while fechting JOB', exc_info=False)

    if job and not job.url_job:
        logger.warning('Invalid url_job. There is not Job URL')
        return None

    return job


def _fetch_job_by_category(job_site_uid: str, host: str, link: str):
    """
    Fetches jobs from the job_site_uid and link provided

    :param job_site_uid: The unique identifier for a job site
    :param host: The website host
    :param link: The relative url
    """
    logger.info('Start fetching job at follow category:  {}'.format(link))
    category = None

    # Try to fetch the job offer page and parse it. If it fails, return None.
    try:
        category = jobs.OfferJobsByCategoryPage(job_site_uid, _build_link(host, link))
    except(HTTPError, MaxRetryError):
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

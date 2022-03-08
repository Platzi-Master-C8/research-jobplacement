# Python
import requests
import datetime

# BeautifulSoup4
import bs4

# Config
from common import config


class JobPage:
    """
    Class to represent a job page.

    :param job_site_uid: The job site uid.
    :param host: The host of the job site.
    """

    def __init__(self, job_site_uid: str, host: str):
        self._urlSite = host
        self._currentDT = datetime.datetime.now()

        self._config = config()['job_sites'][job_site_uid]
        self._queries = self._config['queries']
        self._html = None
        self._visit(self._urlSite)

    def _find(self, query_string: str):
        """
        Finds the first element that matches the query string.

        :param query_string: The query string to search for.
        :return: The first element that matches the query string.
        """
        return self._html.find("meta", property=query_string)

    def _find_description(self, query_string: str):
        """
        Finds the description of the job.

        :param query_string: The query string to search for.
        :return: The description of the job.
        """
        return self._html.find("div", {"class": query_string})

    def _select(self, query_string: str):
        """
        Selects the first element that matches the query string.

        :param query_string: The query string to search for.
        :return: The first element that matches the query string.
        """
        return self._html.select(query_string)

    def _visit(self, url: str):
        """
        Visits the url and stores the html.

        :param url: The url to visit.
        """
        response = requests.get(url)
        response.raise_for_status()
        self._html = bs4.BeautifulSoup(response.text, 'html.parser')


class HomePage(JobPage):
    """
    Class to represent the home page.

    :param job_site_uid: The uid of the job site.
    :param host: The host of the job site.
    """

    def __init__(self, job_site_uid, host):
        super().__init__(job_site_uid, host)

    @property
    def jobCategories_links(self):
        """
        Gets the links to the job categories.

        :return: The links to the job categories.
        """
        link_list = []
        for link in self._select(self._queries['homepage_category_links']):
            link_list.append(link.a['href'])
        return link_list


class OfferJobsByCategoryPage(JobPage):
    """
    Class to represent the offer jobs by category page.

    :param job_site_uid: The uid of the job site.
    :param host: The host of the job site.
    """

    def __init__(self, job_site_uid: str, host: str):
        super().__init__(job_site_uid, host)

    @property
    def jobOffer_links(self):
        """
        Gets the links to the job offers.

        :return: The links to the job offers.
        """
        link_list = []
        for link in self._select(self._queries['jobs_by_category_links']):
            link_list.append(link['href'])

        return link_list


class OfferJobPage(JobPage):
    """
    Class to represent the offer job page.

    :param job_site_uid: The uid of the job site.
    :param host: The host of the job site.
    """

    def __init__(self, job_site_uid: str, host: str):
        super().__init__(job_site_uid, host)

    @property
    def url_job(self):
        """
        Gets the url of the job.

        :return: The url of the job.
        """
        result = self._find(self._queries['job_url'])
        return result['content']

    @property
    def title_job(self):
        """
        Gets the title of the job.

        :return: The title of the job.
        """
        result = self._select(self._queries['job_title'])
        return result[0].text if len(result) else ''

    @property
    def company_job(self):
        """
        Gets the company of the job.

        :return: The company of the job.
        """
        result = self._select(self._queries['job_company'])
        return result[0].text if len(result) else ''

    @property
    def public_job_day(self):
        """
        Gets the public job day of the job.

        :return: The public job day of the job.
        """
        result = self._select(self._queries['job_public_day'])
        return result[0]['datetime'] if len(result) else ''

    @property
    def job_title_category_jobPlace(self):
        """
        Gets the job title, category and job place of the job.

        :return: The job title, category and job place of the job.
        """
        jobtitle_category_jobplace_list = []
        for category in self._select(self._queries['list_with_jobtype_category_jobplace']):
            jobtitle_category_jobplace_list.append(category)
        return jobtitle_category_jobplace_list

    @property
    def job_description(self):
        """
        Gets the job description of the job.

        :return: The job description of the job.
        """
        result = self._find_description(self._queries['jobdescription'])
        return result

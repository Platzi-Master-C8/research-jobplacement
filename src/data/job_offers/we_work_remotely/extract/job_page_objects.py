import bs4
import requests
import datetime

from common import config

class JobPage:

    def __init__(self, job_site_uid, host):
        self._urlSite = host
        self._currentDT = datetime.datetime.now()

        self._config = config()['job_sites'][job_site_uid]
        self._queries = self._config['queries']
        self._html = None
        self._visit(self._urlSite)

    def _find(self, query_string):
        return self._html.find("meta", property=query_string)

    def _find_description(self, query_string):
        return self._html.find("div", { "class" : query_string })

    def _select(self, query_string):
        return self._html.select(query_string)

    def _visit(self, url):
        response = requests.get(url)
        response.raise_for_status()
        self._html = bs4.BeautifulSoup(response.text, 'html.parser')


        #with requests.Session() as s:
            #s.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36'
            #r = s.post(self._urlLogin, data=self._post_data)
            #r.raise_for_status()
            #sendFilter = s.post(self._urlDetailSells, data=self._filter_data)
            #self._html = bs4.BeautifulSoup(sendFilter.text, 'html.parser')
            #print(sendFilter.text) 
        #    response = requests.get(url)
        #    response.raise_for_status()
        #    self._html = bs4.BeautifulSoup(response.text, 'html.parser')

class HomePage(JobPage):

    def __init__(self, job_site_uid, host):
        super().__init__(job_site_uid, host)

    @property
    def jobCategories_links(self):
        link_list = []
        for link in self._select(self._queries['homepage_category_links']):
            link_list.append(link.a['href'])
        return link_list
        #return set(link['href'] for link  in link_list)


class Offer_jobs_by_Category_Page(JobPage):

    def __init__(self, job_site_uid, host):
        super().__init__(job_site_uid, host)

    @property
    def jobOffer_links(self):
        link_list = []
        for link in self._select(self._queries['jobs_by_category_links']):
            link_list.append(link['href'])

        return link_list

class Offer_job_Page(JobPage):

    def __init__(self, job_site_uid, host):
        super().__init__(job_site_uid, host)

    @property
    def url_job(self):
        result = self._find(self._queries['job_url'])
        return result['content']

    @property
    def title_job(self):
        result = self._select(self._queries['job_title'])
        return result[0].text if len(result) else ''

    @property
    def company_job(self):
        result = self._select(self._queries['job_company'])
        return result[0].text if len(result) else ''

    @property
    def public_job_day(self):
        result = self._select(self._queries['job_public_day'])
        return result[0]['datetime'] if len(result) else ''

    @property
    def jobTitle_Category_JobPlace(self):
        jobTitle_Category_JobPlace_List = []
        for property in self._select(self._queries['list_with_jobtype_category_jobplace']):
            jobTitle_Category_JobPlace_List.append(property)
        return jobTitle_Category_JobPlace_List

    @property
    def job_description(self):
        result = self._find_description(self._queries['jobdescription'])
        #print(type(result))
        #print(len(result))
        return result


    


# Python
import datetime as dt

# Urllib
import urllib.request as urllib2

# Pandas
import pandas as pd

# BeautifulSoup
from bs4 import BeautifulSoup

# Config
from data.job_offers.remote_ok.common import config

# Utils
from utils.files import save_data

# Logging
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class NewOffer:
    """
    Class to get new offers from remote.ok and save them to csv file.
    """

    def __init__(self):
        self._config = config()['newoffers']
        self._queries = self._config['remoteok']['queries']
        self.url = self._config['remoteok']['url']
        self.soup = self._get_html(self.url)
        self._get_data(self.soup, self._queries)

    def _get_html(self, url):
        """
        Get html from url.

        :param url: url to get html from.
        """
        opener = urllib2.build_opener()
        opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
        website = opener.open(url)
        html = website.read()
        logger.info('Getting html from url: {}'.format(url))
        soup = BeautifulSoup(html, "html.parser")
        return soup

    def get_description(self, urls: list) -> list:
        """
        Get description from url.

        :param urls: url to get description from.
        :return: descriptions.
        """
        descriptions = []
        for url in urls:
            soup = self._get_html(url)
            description = [str.strip(p.text) for p in soup.select(
                self._queries['description'])]
            descriptions.append(description)

        return descriptions

    def get_skills(self, containers):
        """
        Get skills from html.

        :param containers: html to get skills from.
        :return: skills.
        """
        skills = []
        i = 0
        for container in containers:
            skill_v = []

            skill_list = container.find_all('div', rel='follow')
            for skill in skill_list:
                a = str.strip(skill.text)
                skill_v.append(a)
                i += 1

            skills.append(skill_v)

        skills.pop(0)
        return skills

    def _get_data(self, soup, queries: dict):
        """
        Get data from html and save it to csv file.

        :param soup: html to get data from.
        :param queries: queries to get data from.
        """
        skills = self.get_skills(soup.select(queries['skills_container']))
        position = [str.strip(p.text) for p in soup.select(queries['position_name'])]
        enterprise = [str.strip(p.text) for p in soup.select(queries['enterprise_name'])]
        location = [str.strip(p.text) for p in soup.select(queries['location_salary']) if
                    not (str.strip(p.text)).startswith('💰 ')]
        salary = [str.strip(p.text) for p in soup.select(
            queries['location_salary']) if (str.strip(p.text)).startswith('💰 ')]
        publish = [i['datetime'] for i in soup.select(
            queries['publish_date']) if i.has_attr('datetime')]
        position_url = ['https://remoteok.com' + h['href']
                        for h in soup.select(queries['offer_url'])]
        description = self.get_description(position_url)

        df = pd.DataFrame(
            list(zip(position, enterprise, salary, location, publish, position_url, description, skills)),
            columns=['Posicion', 'Empresa', 'Salario', 'Ubicacion', 'Fecha de publicacion', 'URL de la vacante',
                     'Descripción', 'skills'])
        df['Home URL'] = 'https://remoteok.com'
        df['Nombre del Sitio'] = 'REMOTEOK'
        today = dt.date.today()
        save_data(df, f'REMOTEOK_{today}_offers.csv', 'raw')

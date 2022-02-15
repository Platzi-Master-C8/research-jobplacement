import urllib.request as urllib2
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
# from src.data.job_offers.remote_ok.common import config
from common import config


class NewOffer:
    def __init__(self):
        self._config = config()['newoffers']
        self._queries = self._config['remoteok']['queries']
        self.url = self._config['remoteok']['url']
        self.soup = self._get_html(self.url)
        self._get_data(self.soup, self._queries)

    def _get_html(self, url):
        opener = urllib2.build_opener()
        opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
        website = opener.open(url)
        html = website.read()
        soup = BeautifulSoup(html, "html.parser")
        return soup

    def get_description(self, urls):
        descriptions = []
        for url in urls:
            soup = self._get_html(url)
            description = [str.strip(p.text) for p in soup.select(
                self._queries['description'])]
            descriptions.append(description)

        return descriptions

    def get_skills(self, containers):
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
            # print('___'*15)
        skills.pop(0)
        # print((skills))
        return skills

    def _get_data(self, soup, queries):
        # skills = [str.strip(p.text) for p in soup.select(queries['skills'])]
        skills = self.get_skills(soup.select(queries['skills_container']))
        position = [str.strip(p.text)
                    for p in soup.select(queries['position_name'])]
        enterprise = [str.strip(p.text)
                      for p in soup.select(queries['enterprise_name'])]
        location = [str.strip(p.text) for p in soup.select(
            queries['location_salary']) if not (str.strip(p.text)).startswith('💰 ')]
        sallary = [str.strip(p.text) for p in soup.select(
            queries['location_salary']) if (str.strip(p.text)).startswith('💰 ')]
        publish = [i['datetime'] for i in soup.select(
            queries['publish_date']) if i.has_attr('datetime')]
        position_url = ['https://remoteok.com'+h['href']
                        for h in soup.select(queries['offer_url'])]
        description = self.get_description(position_url)
        df = pd.DataFrame(list(zip(position, enterprise, sallary, location, publish, position_url, description, skills)),
                          columns=['Posicion', 'Empresa', 'Salario', 'Ubicacion', 'Fecha de publicacion', 'URL de la vacante', 'Descripción', 'skills'])
        df['Home URL'] = 'https://remoteok.com'
        df['Nombre del Sitio'] = 'REMOTEOK'
        today = dt.date.today()
        df.to_csv(
            f'./data/raw/REMOTEOK_{today}_offers.csv', encoding='utf-8-sig', index=False)


if __name__ == '__main__':
    a = NewOffer()

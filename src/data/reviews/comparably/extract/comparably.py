# Python
import os

# Pandas
import pandas as pd

# asyncio
import asyncio

# PyYaml
import yaml

# Pyppeteer
from pyppeteer import launch

# BeautifulSoup
from bs4 import BeautifulSoup

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


class ComparablyWebScraping:

    def __init__(self):
        self.url = 'https://co.indeed.com/cmp/Adobe'
        self.browser = None
        self.page = None
        self.__config = None
        self.config_path = os.path.abspath(f'{ROOT_DIR}/config.yaml')

    def config(self):
        if not self.__config:
            with open(self.config_path, mode='r') as f:
                self.__config = yaml.load(f, Loader=yaml.FullLoader)
        return self.__config

    async def get_browser(self):
        return await launch({'devtools': True})

    async def close_browser(self):
        return await self.browser.close()

    async def _page_evaluate(self, query: str):
        query_result = await self.page.evaluate(
            pageFunction=query,
            force_expr=True
        )
        return query_result

    async def get_companies_most_rated(self) -> list:
        config_yml = self.config()['job_sites']['comparably']['queries']
        self.browser = await self.get_browser()
        self.page = await self.browser.newPage()
        await self.page.goto(self.url)
        await self.page.mouse.down({'button': 'middle'})
        await self.page.screenshot({'path': 'example.png'})
        await asyncio.sleep(60)
        await self.page.screenshot({'path': 'loading.png'})
        await self.page.click('a.mostRated')

        await asyncio.sleep(1)
        company_list = await self._page_evaluate(query=config_yml['companies_links'])
        soup = BeautifulSoup(company_list)
        company_links = [i['href'] for i in soup.find_all(class_='companyLink')]
        return company_links

    async def search_data(self, url: str) -> dict:
        config_yml = self.config()['job_sites']['comparably']['queries']
        company_info = {
            'name': await self._page_evaluate(query=config_yml['company_name']),
            'ceo': await self._page_evaluate(query=config_yml['ceo_name']),
            'ceo_score': await self._page_evaluate(query=config_yml['ceo_score']),
            'employee_participants': await self._page_evaluate(query=config_yml['employee_participants']),
            'total_ratings': await self._page_evaluate(query=config_yml['total_ratings']),
            'culture_score': await self._page_evaluate(query=config_yml['culture_score']),
        }

        await self.page.goto(f'{url}/reviews')
        await asyncio.sleep(1)
        score_info = {
            'score_positive_reviews': await self._page_evaluate(query=config_yml['score_positive_reviews']),
            'score_negative_reviews': await self._page_evaluate(query=config_yml['score_negative_reviews']),
        }
        await asyncio.sleep(1)
        about_url = await self._page_evaluate(query=config_yml['about_url'])
        await self.page.goto(about_url)
        about_company = {
            'description': await self._page_evaluate(query=config_yml['company_description']),
            'website': await self._page_evaluate(query=config_yml['company_website']),
        }

        cleaned_data = {**company_info, **score_info, **about_company}
        return cleaned_data

    async def get_company_reputation(self, companies_urls: list) -> list:
        self.page = await self.browser.newPage()
        data = []
        for url in companies_urls:
            await self.page.goto(url)
            await asyncio.sleep(1)
            data.append(await self.search_data(url))
        await self.close_browser()
        return data


async def extract_data():
    scraping_comparably = ComparablyWebScraping()

    links = await scraping_comparably.get_companies_most_rated()
    data = await scraping_comparably.get_company_reputation(links)
    df = pd.DataFrame(data)
    return df

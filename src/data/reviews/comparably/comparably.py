# Pandas
import pandas as pd

# nest_asyncio
import nest_asyncio
import asyncio

# PyYaml
import yaml

# Pyppeteer
from pyppeteer import launch

# BeautifulSoup
from bs4 import BeautifulSoup


class ComparablyWebScraping:

    def __init__(self):
        self.url = 'https://www.comparably.com/companies'
        self.browser = None
        self.page = None
        self.__config = None

    def config(self):
        if not self.__config:
            with open('/content/gdrive/MyDrive/config.yaml', mode='r') as f:
                self.__config = yaml.load(f)
        return self.__config

    async def get_browser(self):
        return await launch(executablePath="/usr/lib/chromium-browser/chromium-browser", args=['--no-sandbox'])

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
        await self.page.click('a.mostRated')
        await asyncio.sleep(1)
        company_list = await self._page_evaluate(query=config_yml['companies_links'])
        soup = BeautifulSoup(company_list)
        company_links = [i['href'] for i in soup.find_all(class_='companyLink')]
        return company_links

    async def search_data(self, url: str) -> dict:
        config_yml = self.config()['job_sites']['comparably']['queries']
        company_info = {
            'company_name': await self._page_evaluate(query=config_yml['company_name']),
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

        cleaned_data = {**company_info, **score_info}
        print(cleaned_data)
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


async def main():
    scraping_comparably = ComparablyWebScraping()

    links = await scraping_comparably.get_companies_most_rated()
    data = await scraping_comparably.get_company_reputation(links)
    df = pd.DataFrame(data)
    return df


nest_asyncio.apply()
asyncio.get_event_loop().run_until_complete(main())

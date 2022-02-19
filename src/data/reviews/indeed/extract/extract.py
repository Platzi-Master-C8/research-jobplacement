# Python
import os

# Pandas
from datetime import datetime

import pandas as pd

# asyncio
import asyncio
import nest_asyncio

# PyYaml
import yaml

# Pyppeteer
from pyppeteer import launch

# Utils
from pyppeteer.errors import ElementHandleError, NetworkError

from utils.db_connector import connect_to_db
from utils.files import save_data

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


class IndeedWebScraping:
    """
    Indeed Web Scraping Class for extracting data from Indeed.com
    """

    def __init__(self):
        self.url = 'https://www.indeed.com/companies'
        self.browser = None
        self.page = None
        self.__config = None
        self.companies_to_search = None
        self.config_path = os.path.abspath(f'{ROOT_DIR}/properties.yaml')
        self.properties_yml = self.config()['indeed_properties']['queries']

    def config(self):
        """
        Loads the configuration file and returns the data as a dictionary
        """
        if not self.__config:
            with open(self.config_path, mode='r') as f:
                self.__config = yaml.load(f, Loader=yaml.FullLoader)
        return self.__config

    async def get_browser(self):
        """ Returns a browser instance """
        return await launch(
            {'devtools': True, 'ignoreHTTPSErrors': True, 'defaultViewport': {'width': 1920, 'height': 1080}})

    async def close_browser(self):
        """ Closes the browser instance """
        return await self.browser.close()

    async def _page_evaluate(self, query: str):
        """ Evaluates the query in the page """
        query_result = await self.page.evaluate(
            pageFunction=query,
            force_expr=True
        )
        return query_result

    def get_companies_to_search(self):
        """ Returns a list of companies to search """
        engine = connect_to_db()
        df_companies_to_search = pd.read_sql_query(
            'SELECT id_company, name FROM company WHERE death_line IS NULL LIMIT 10', engine)
        self.companies_to_search = df_companies_to_search.to_dict('records')

    async def search_data(self):
        """ Searches the data """
        companies_info = []
        companies_reviews = {}
        self.browser = await self.get_browser()
        self.page = await self.browser.newPage()
        self.get_companies_to_search()
        for company in self.companies_to_search:
            await self.page.goto(self.url)
            try:
                await self.search_company(company.get('name'))
                await self.page.waitFor(3000)
                companies_info.append(await self.get_company_info())
                reviews = await self.get_company_reviews(company.get('id_company'))
                companies_reviews = {**companies_reviews, **reviews}
            except ElementHandleError as e:
                print(f'ElementHandleError: {company.get("name")}')
                continue
            except NetworkError:
                print(f'NetworkError: {company.get("name")}')
                continue
        await self.close_browser()
        return companies_info, companies_reviews

    async def search_company(self, company_name: str):
        """ Searches a company """
        properties_yml = self.properties_yml

        # Search the company name in the search bar
        await self.page.type(properties_yml['input_search'], company_name)
        await self.page.click(properties_yml['button_search'])
        await self.page.waitFor(3000)

        # Click on the first company in the list
        company_url = await self._page_evaluate(query=properties_yml['first_result'])
        await self.page.goto(f'{company_url}/reviews')
        await self.page.waitFor(3000)

    async def get_company_info(self):
        """
        Returns the company information as a dictionary
        """
        properties_yml = self.properties_yml

        # Get the company info from the page
        name = await self._page_evaluate(query=properties_yml['company_name'])
        company_overall = await self._page_evaluate(query=properties_yml['company_overall'])
        total_ratings = await self._page_evaluate(query=properties_yml['total_ratings'])
        culture_score = await self._page_evaluate(query=properties_yml['culture_score'])
        work_life_balance_score = await self._page_evaluate(query=properties_yml['work_life_balance_score'])
        career_opportunities_score = await self._page_evaluate(query=properties_yml['career_opportunities_score'])
        perks_score = await self._page_evaluate(query=properties_yml['perks_score'])
        return {
            'name': name,
            'avg_reputation': company_overall,
            'total_ratings': total_ratings,
            'death_line': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'culture_score': culture_score,
            'work_life_balance_score': work_life_balance_score,
            'career_opportunities_score': career_opportunities_score,
            'perks_score': perks_score,
        }

    async def get_company_reviews(self, company_id: int):
        """ Gets the reviews of a company """
        properties_yml = self.properties_yml

        # Get the reviews from the page and save them in a list
        reviews_length = await self._page_evaluate(query=properties_yml['reviews'])
        reviews = {}
        reviewElement = 'document.getElementsByClassName("css-5cqmw8")'

        # Iterate over the reviews and get the info of each one of them
        for i in range(reviews_length):
            review_title = await self._page_evaluate(
                query=f'{reviewElement}[{i}].{properties_yml["review_title"]}'
            )
            user_info = await self._page_evaluate(
                query=f'{reviewElement}[{i}].{properties_yml["user_info"]}'
            )
            content_type_lengt = await self._page_evaluate(
                query=f'{reviewElement}[{i}].{properties_yml["content_type_length"]}'
            )
            content_type = await self._page_evaluate(
                query=f'{reviewElement}[{i}].children[1].children[1].children[{content_type_lengt - 2}].innerText'
            )
            review_score = await self._page_evaluate(
                query=f'{reviewElement}[{i}].{properties_yml["review_score"]}'
            )
            review = {
                'company_id': company_id,
                'review_title': review_title,
                'user_info': user_info,
                'content_type': content_type,
                'review_score': review_score,
            }
            reviews[i] = review

        return reviews


async def extract_data():
    scraping_indeed = IndeedWebScraping()
    companies_info, companies_reviews = await scraping_indeed.search_data()
    df_companie_info = pd.DataFrame(companies_info)
    df_company_reviews = pd.DataFrame(companies_reviews)
    save_data(df_companie_info, 'indeed_companies_info.csv', 'raw')
    save_data(df_company_reviews, 'indeed_companies_reviews.csv', 'raw')


if __name__ == '__main__':
    nest_asyncio.apply()
    x = asyncio.get_event_loop().run_until_complete(extract_data())

# Asyncio
import asyncio
import nest_asyncio

# Logging
import logging

# Process data
from data.reviews.indeed.extract.extract import extract_data
from data.reviews.indeed.transform.transform import transform

# Utils
from utils.files import read_data
from utils.interface import PipelineInterface
from utils.db_connector import connect_to_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IndeedPipeline(PipelineInterface):
    """
    Class to process Indeed data and store it in a database. The pipeline is composed of the following steps:

    1. Extract data from the source
    2. Transform data
    3. Store data in a database
    """

    def __init__(self):
        self.connection = connect_to_db()

    def execute(self):
        """
        Execute the pipeline to extract, transform and load data into the database
        """
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        """
        Extract data from indeed.com
        """
        logger.info('Extracting data from indeed.com')
        nest_asyncio.apply()
        asyncio.get_event_loop().run_until_complete(extract_data())

    def transform(self):
        """
        Transform data from the database into a more usable format
        """
        logger.info('Transforming data to fit the database')
        transform()

    def load(self):
        """
        Load data into the database from the transformed data
        """
        logger.info('Loading data into the database')
        df_reviews = read_data('indeed_companies_reviews.csv', 'processed')
        df_companies = read_data('indeed_companies_info.csv', 'raw')
        # Upload the user reviews to the database
        df_reviews.to_sql('user_review', con=self.connection, if_exists='append', index=False)
        logger.info(f'Loaded {len(df_reviews)} reviews')

        # Update companies table with new data
        for index, row in df_companies.iterrows():
            query = f"""
                UPDATE company
                SET
                avg_reputation = {row['avg_reputation']},
                total_ratings = {row['total_ratings']},
                death_line = '{row['death_line']}',
                culture_score = {row['culture_score']},
                work_life_balance_score = {row['work_life_balance_score']},
                career_opportunities_score = {row['career_opportunities_score']},
                perks_score = {row['perks_score']}
                WHERE name = '{row['name']}'
            """
            self.connection.execute(query)
        logger.info(f'Updated {len(df_companies)} companies')
        logger.info('Data loaded successfully into the database')

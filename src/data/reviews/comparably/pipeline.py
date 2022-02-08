# Nest asyncio
import asyncio
import nest_asyncio

# Utils
from utils.interface import PipelineInterface
from utils.files import (save_data, read_data)

# Extraction
from .extract.comparably import extract_data


class ComparablyPipeline(PipelineInterface):

    def __init__(self):
        pass

    def execute(self):
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        nest_asyncio.apply()
        x = asyncio.get_event_loop().run_until_complete(extract_data())
        save_data(x, 'data/reviews/comparably/data.json')

    def transform(self):
        pass

    def load(self):
        pass

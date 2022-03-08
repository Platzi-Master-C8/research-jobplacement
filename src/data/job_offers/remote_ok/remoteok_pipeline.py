# Logger
import logging
# Pipeline and Data
import data.job_offers.remote_ok.new_offers as ro_ex
import data.job_offers.remote_ok.clean as ro_tr
import data.job_offers.remote_ok.load as ro_lo

# Utils
from utils.interface import PipelineInterface

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RemoteokPipeline(PipelineInterface):
    def __init__(self):
        pass

    def execute(self):
        """
        Execute the pipeline in order to extract, clean and load the data
        """
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        """
        Extract the data from remoteok website and save it in csv files
        """
        logger.info("Extracting new remoteok offers")
        ro_ex.NewOffer()

    def transform(self):
        """
        Clean the data and save it in the database (if needed) and load it
        """
        logger.info("Cleaning remoteok offers")
        ro_tr.Clean()

    def load(self):
        """
        Load the data in the database (if needed) and save it in csv files
        """
        logger.info("Loading remoteok offers")
        ro_lo.Load()

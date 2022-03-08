# Python
import subprocess
import datetime
import shutil

# Logging
import logging

# Utils
from utils.interface import PipelineInterface

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeWorkRemotelyPipeline(PipelineInterface):
    """
    Pipeline for the WeWorkRemotely dataset.
    """

    def __init__(self):
        self.weworkremotely_site = 'weworkremotely'
        self.currentDT = datetime.datetime.now()
        self.currentDateTime = str(self.currentDT.year) + "_" + str(self.currentDT.month) + "_" + str(
            self.currentDT.day)
        self.fileNameToTransform = self.weworkremotely_site + "_" + self.currentDateTime + "_jobs"
        self.cleanFileNameToLoad = 'clean_' + self.fileNameToTransform
        self.perksFileName = 'perks' + "_" + self.currentDateTime + "_company"

    def execute(self):
        # self.extract()
        self.transform()
        self.load()

    def extract(self):
        """
        Extracts the data from the website. The data is saved in a csv file.
        """
        logger.info('Starting Extract process to {}'.format(self.currentDateTime))
        subprocess.call(['python', 'main.py', self.weworkremotely_site], cwd='./extract')
        extract_raw_data_filename = './extract/{}.csv'.format(self.fileNameToTransform)
        transform_raw_data_filename = './transform/{}.csv'.format(self.fileNameToTransform)
        logger.info('File Format is {}.csv'.format(self.fileNameToTransform))
        shutil.copy2(extract_raw_data_filename, transform_raw_data_filename)

    def transform(self):
        """
        Transforms the data from the website. The data is saved in a csv file.
        """
        logger.info('Starting Transform process to {}'.format(self.currentDateTime))
        dirty_data_filename = '{}.csv'.format(self.fileNameToTransform)
        subprocess.run(['python', 'position_receipe.py', dirty_data_filename], cwd='./transform')
        logger.info('File Format is {}.csv'.format(self.cleanFileNameToLoad))
        shutil.copy2('./transform/' + '{}.csv'.format(self.perksFileName),
                     './load/' + '{}.csv'.format(self.perksFileName))
        shutil.copy2('./transform/' + '{}.csv'.format(self.cleanFileNameToLoad),
                     './load/' + '{}.csv'.format(self.cleanFileNameToLoad))

    def load(self):
        """
        Loads the data from the website. The data is saved in a csv file.
        """
        logger.info('Starting Load process to {}'.format(self.currentDateTime))
        clean_data_filename = '{}.csv'.format(self.cleanFileNameToLoad)
        subprocess.run(['python', 'main.py', clean_data_filename], cwd='./load')
        perksFileNameCSV = '{}.csv'.format(self.perksFileName)
        subprocess.run(['python', 'loadperks.py', perksFileNameCSV], cwd='./load')


if __name__ == '__main__':
    WeWorkRemotelyPipeline().execute()

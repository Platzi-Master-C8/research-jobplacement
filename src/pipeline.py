# Logger
import logging

# Scrappers
from data.job_offers import (
    GetOnBoardPipeline,
    WeWorkRemotelyPipeline,
    remoteOkPipeline,
)
from data.reviews import (
    IndeedPipeline,
)
from data.job_offers.remote_ok.remoteok_pipeline import (
    RemoteokPipeline,

from data.companies import CompaniesGetOnBoardPipeline

# Utils
from utils.interface import PipelineInterface

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    pipelines = [
        GetOnBoardPipeline(),
        IndeedPipeline(),
        CompaniesGetOnBoardPipeline(),
        WeWorkRemotelyPipeline(),
        RemoteokPipeline(),
    ]
    for pipeline in pipelines:
        if isinstance(pipeline, PipelineInterface):
            logger.info(f'Starting pipeline {pipeline.__class__.__name__}')
            pipeline.execute()
            logger.info(f'Ending pipeline {pipeline.__class__.__name__}')
        else:
            raise TypeError(f'{pipeline} is not an instance of PipelineInterface')


if __name__ == '__main__':
    main()

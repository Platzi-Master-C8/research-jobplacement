# Logger
import logging

# Scrappers
from data.job_offers import (
    GetOnBoardPipeline,
)
from data.reviews import (
    ComparablyPipeline,
)
from data.job_offers.remote_ok.remoteok_pipeline import (
    RemoteokPipeline,

)

# Utils
from utils.interface import PipelineInterface

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    pipelines = [
        GetOnBoardPipeline(),
        ComparablyPipeline(),
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

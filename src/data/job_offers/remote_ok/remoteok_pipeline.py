
# from abc import abstractmethod
import data.job_offers.remote_ok.new_offers as ro_ex
import data.job_offers.remote_ok.clean as ro_tr
import data.job_offers.remote_ok.load as ro_lo

from utils.interface import PipelineInterface

class RemoteokPipeline(PipelineInterface):
    def __init__(self):
        self.execute

    def execute(self):
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        ro_ex.NewOffer()

    def transform(self):
        ro_tr.Clean()

    def load(self):
        ro_lo.Load()



if __name__ == '__main__':
    a = RemoteokPipeline()
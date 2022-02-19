from utils.interface import PipelineInterface


class IndeedPipeline(PipelineInterface):

    def __init__(self):
        pass

    def execute(self):
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass

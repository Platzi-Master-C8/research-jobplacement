class PipelineInterface:
    def execute(self):
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        """ Extract data from the data source"""
        pass

    def transform(self):
        """ Transform data from the data source extracted"""
        pass

    def load(self):
        """ Load the transformed data into the database"""
        pass

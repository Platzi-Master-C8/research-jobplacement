import yaml 

__config = None

def config():
    #global __config
    if not __config:
        with open('./src/data/job_offers/remote_ok/config.yaml') as f:
            config = yaml.safe_load(f)

        return config
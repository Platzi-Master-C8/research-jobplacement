# Python
import os
import yaml

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
__config = None


def config():
    """
    Returns the config object. If it doesn't exist, it will be created.

    :return: The config object.
    """
    global __config
    if not __config:
        with open(f'{ROOT_DIR}/config.yaml') as f:
            __config = yaml.safe_load(f)

    return __config

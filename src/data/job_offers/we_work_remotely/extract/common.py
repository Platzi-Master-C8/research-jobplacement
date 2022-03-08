# Python
import yaml

__config = None


def config():
    """
    Returns the config object. If it doesn't exist, it will be created.

    :return: The config object.
    """
    global __config
    if not __config:
        with open('config.yaml', mode='r') as f:
            __config = yaml.load(f, Loader=yaml.FullLoader)

    return __config

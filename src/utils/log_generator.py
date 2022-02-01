# Python
from datetime import datetime
# Logger
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Line to be logged
LINE = """\
[{time_local} +0000] "{request_type} {request_path} HTTP/1.1" {status}"\
"""


def generate_log_line(response):
    """
    Generate a log line from a response object.
    """
    now = datetime.now()
    time_local = now.strftime('%d/%b/%Y:%H:%M:%S')
    request_type = response.request.method
    request_path = response.url
    status = response.status_code

    log_line = LINE.format(
        time_local=time_local,
        request_type=request_type,
        request_path=request_path,
        status=status,
    )
    logger.info(log_line)

    return log_line

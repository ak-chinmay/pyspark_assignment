import sys
from jobs.director import Director
from jobs.exceptions import SparkConnectionException
from jobs.util.process_log import ProcessLog

if __name__ == '__main__':
    """
    This block is responsible for executing the complete program
    """
    try:
        Director().run()
    except SparkConnectionException as se:
        ProcessLog().logger.error(se.__str__())
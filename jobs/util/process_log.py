import logging


class ProcessLog:
    """
    This class creates the logger object and configures it.
    """
    logger = None
    ASSIGNMENT = "assignment"

    def __init__(self):
        """
        This constructor configures the Logging.
        """
        self.logger = logging.getLogger(self.ASSIGNMENT)
        self.logger.setLevel("INFO")
        consoleLog = logging.StreamHandler()
        self.logger.addHandler(consoleLog)

    def getLogger(self):
        """
        This method is a getter for logger object.

        :return: logger object
        """
        return self.logger

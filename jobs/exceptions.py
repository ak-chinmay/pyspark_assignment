class PropertiesNotFound(Exception):
    """
    This exception will be thrown if the property in the properties file is not found.
    """
    def __init__(self, arg):
        self.strerror = arg
        self.args = {arg}

class SparkConnectionException(IOError):
    """
    This exception will be thrown if the property in the properties file is not found.
    """
    def __init__(self, arg):
        self.strerror = arg
        self.args = {arg}


class EmptySourceException(ValueError):
    """
    This exception will be thrown if the property in the properties file is not found.
    """
    def __init__(self, arg):
        self.strerror = arg
        self.args = {arg}

class PropertyNotFoundException(IOError):
    """
    This exception will be thrown if the property in the properties file is not found.
    """
    def __init__(self, arg):
        self.strerror = arg
        self.args = {arg}

class DataFrameNoneException(RuntimeError):
    """
    This exception will be thrown if the property in the properties file is not found.
    """
    def __init__(self, arg):
        self.strerror = arg
        self.args = {arg}

class IllegalArgumentException(ValueError):
    """
    This exception will be thrown if the property in the properties file is not found.
    """
    def __init__(self, arg):
        self.strerror = arg
        self.args = {arg}

class NoDataFoundException(ValueError):
    """
       This exception will be thrown if the data is not found in the result.
       """

    def __init__(self, arg):
        self.strerror = arg
        self.args = {arg}


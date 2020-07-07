import json

from jobs.exceptions import PropertiesNotFound


class PropertiesReader:
    """
    This class is responsible for reading the Properties.json file.
    """
    def get_properties_data(self):
        """
        This methods reads the properties file and returns a dict object.

        :param propertiesFilePath: path of the Properties file accepted through CLI arguments
        :return: dict object containing the properties data
        """
        propertiesFilePath = "./config.json"
        with open(propertiesFilePath) as json_file:
            data = json.load(json_file)
            if data is None:
                raise PropertiesNotFound("Problem in accessing properties file")
        return data

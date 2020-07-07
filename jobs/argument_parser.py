import argparse

import sys

from jobs.exceptions import IllegalArgumentException


class ArgumentParser:
    """
    This class is responsible for parsing the arguments by user
    """

    def parse(self):
        """
        This method parses the user argument and returns the extracted data
        :return: customer_id, drugId, test variable arguments
        """
        args = self.getArgs()
        customer_id = args.customer_id
        file_path = args.file_path
        test = args.test

        if customer_id == None and test == False  :
            raise IllegalArgumentException(
                "Please provide at least one required argument, pass -h or --help argument for more info.")
        sys.argv = [sys.argv[0]]
        return customer_id, file_path, test

    def getArgs(self):
        """
        This method holds the business logic of parsing the argument
        :return: args the object holding the user arguments
        """
        parser = argparse.ArgumentParser(
            description='This program finds the similar customers for a perticular customer id')
        parser.add_argument("-c", help="Customer id to compare to, (required)", dest="customer_id")
        parser.add_argument("-f",
                            help="The path to the data file, (if not given default value will be taken from properties file)",
                            dest="file_path")
        parser.add_argument("--test", help="Running all the tests of Ideaolo Assignment", action="store_true",
                            default=False)
        return parser.parse_args()

import os
from unittest import main

import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from jobs.argument_parser import ArgumentParser
from jobs.exceptions import IllegalArgumentException, NoDataFoundException
from jobs.tasks.analytics.similarity import Similarity
from jobs.tasks.io.read import ReadFile
from jobs.tasks.io.spark_connection import SparkConnection
from jobs.tasks.io.write import WriteFile
from jobs.util.process_log import ProcessLog
from jobs.util.properties_reader import PropertiesReader


class Director(object):
    """
    This class is the main workflow director class, it handles all execution of workflow and exception handling.
    """

    def __init__(self):
        """
        This constructor initializes logger.
        """
        self.props = PropertiesReader().get_properties_data()
        self.logger = ProcessLog().getLogger()

    def perform_analysis(self):
        """
        This method is responsible for performing analysis which is the main part of the program.
        This prints the output to STDOUT.
        """
        try:
            read_file = ReadFile(self.spark, self.logger)
            if self.file_path is None:
                self.file_path = self.props['LOCAL_INPUT_FILE']
            customers_df = read_file.read_json(self.file_path)
            similarity = Similarity(self.spark, self.logger)
            similar_customers_df = similarity.find_similar_customers(customers_df, self.customer_id)
            write_file = WriteFile(self.spark, self.logger)
            write_file.write_stdout(similar_customers_df)

        except Exception as e:
            self.logger.error(e)

    def parse_user_input(self):
        """
        This method parses user input and assigns it to variables
        """
        self.customer_id, self.file_path, self.test = ArgumentParser().parse()

    def perform_action(self):
        """
        This method performs action depending on the user input
        """
        self.spark = SparkConnection().connectSpark(self.props['SPARK_APP_NAME'], self.props['SPARK_URL'])
        if self.customer_id:
            self.perform_analysis()
        if self.test:
            self.runAllTests()

    def runAllTests(self):
        """
        Runs all the tests
        """
        main(module='jobs.test', exit=True, verbosity=2)

    def run(self):
        """
        This method parses the user input and performs action and prints the status.
        """
        try:
            self.parse_user_input()
            self.perform_action()
        except NoDataFoundException as nd:
            self.logger.error("Error: {}".format(nd))
        except  IllegalArgumentException as ie:
            self.logger.error("Error: {}".format(ie))
        except Exception as e:
            self.logger.error("Error: {}".format(e))

            # TODO: python docs and md

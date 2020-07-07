import os

import pkg_resources
import sys
from pyspark.sql import SparkSession

from jobs.tasks.io.read import ReadFile


class TestUtils:
    """
    This class holds common methods required by the tests.
    """

    @staticmethod
    def get_spark_instance():
        """
        This method creates a SparkSession and returns the object for the tests to use.

        :return: SparkSession object
        """
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        return SparkSession.builder \
            .appName("MyApp") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

    @staticmethod
    def read_file(spark):
        """
        This method creates a SparkSession and returns the object for the tests to use.

        :return: SparkSession object
        """
        input_file = "./tests/resources/assignment-spark-test-data.json"
        if not (os.path.isfile(input_file)):
            input_file = TestUtils.get_resources_path() + '/assignment-spark-test-data.json'
        return ReadFile.instantiate(spark).read_json(input_file)

    @staticmethod
    def get_resources_path():
        """
        This method returns the path to the resources folder
        :return: path_to_resources_folder
        """
        return pkg_resources.resource_filename('tests', 'resources')

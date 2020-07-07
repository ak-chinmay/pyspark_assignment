from pyspark import SQLContext

from jobs.exceptions import DataFrameNoneException


class WriteFile:
    """
    This class is responsible for ReadFile which means to read a file and load it as PySpark data frame.
    """

    def __init__(self, spark, logger):
        """
        This constructor initializes the Spark Session object.

        :param spark: Spark Session object
        """
        self.spark = spark
        self.logger = logger

    def write_csv(self, df, filename, parquet=True, csv=False):
        """
        This method writes df into csv file

        :param df : dataframe
        :param filename
        """

        self.logger.info("# Writing a CSV file "+filename)
        df.write.csv(filename)

    def write_parquet(self, df, filename):
        """
        This method writes df into parquet file

        :param df : dataframe
        :param filename
        """
        if df is None:
            raise DataFrameNoneException("No Dataframe found")
        elif filename is None:
            raise ValueError("File name is None")
        self.logger.info("# Writing a parquet file "+filename)
        df.write.parquet(filename)

    def write_stdout(self, df):
        """
        This method shows df to STDOUT

        :param df : dataframe
        """
        if df is None:
            raise DataFrameNoneException("No Dataframe to display")
        self.logger.info("# Writing a STDOUT")
        df.show()


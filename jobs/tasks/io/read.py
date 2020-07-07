from pyspark import SQLContext

from jobs.util.process_log import ProcessLog


class ReadFile:
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

    @classmethod
    def instantiate(cls, spark):
        """
        This constructor initializes the Spark Session object.

        :param spark: Spark Session object
        """
        logger = ProcessLog().getLogger()
        return cls(spark, logger)

    def read_json(self, filename, multi_line=False, schema=None):
        """
        This method actually loads the input json file

        :param filename: json filename
        :param multi_line: multiLine option
        :param schema: custom schema
        :return: Newly loaded PySpark dataframe
        """
        self.logger.info("# Reading a JSON file " + filename)
        sql_context = SQLContext(self.spark)
        df = None
        if schema is None and multi_line == False:
            df = sql_context.read.json(filename)
        elif multi_line:
            df = sql_context.read.json(filename, multiLine=multi_line)
        elif schema is not None:
            df = sql_context.read.json(filename, schema=schema)
        return df


    def read_parquet(self, filename):
        """
        This method actually loads the input parquet file

        :param filename: input parquet file
        :return: Newly loaded PySpark dataframe
        """
        if filename is None:
            raise ValueError("File name is None")
        self.logger.info("# Reading a Parquet file " + filename)
        sqlContext = SQLContext(self.spark)
        return sqlContext.read.parquet(filename)

    def __str__(self):
        return "FILEToDF"

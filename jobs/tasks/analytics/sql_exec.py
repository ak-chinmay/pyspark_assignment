from pyspark import SQLContext


class SqlExecute:
    """
    This class is responsible for Execution of spark-sql queries
    """

    def __init__(self, spark, logger):
        """
        This constructor initializes the Spark Session object.

        :param spark: Spark Session object
        """
        self.spark = spark
        self.logger = logger

    def execute(self, sql):
        """
        This method actually executes the sql query

        :param sql: SQL query to be executed
        :return: Newly created PySpark dataframe
        """
        if sql is None:
            raise ValueError("Query is None")
        self.logger.info("# Executing an SQL query")
        sqlContext = SQLContext(self.spark)
        df = sqlContext.sql(sql)
        return df


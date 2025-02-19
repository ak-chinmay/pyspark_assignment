from pyspark.sql import SparkSession

from jobs.exceptions import SparkConnectionException


class SparkConnection:
    def __init__(self):
        pass

    def connectSpark(self, spark_app_name, spark_url):
        """
        This method as its name suggests connects to spark
        :return: spark session object
        """
        spark = SparkSession.builder \
            .appName(spark_app_name) \
            .master(spark_url) \
            .enableHiveSupport() \
            .getOrCreate()
        if spark is None:
            raise SparkConnectionException("Unable to connect to Spark: " + spark_url)
        return spark


if __name__ == '__main__':
    pass

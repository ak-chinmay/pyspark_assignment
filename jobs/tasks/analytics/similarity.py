from jobs.exceptions import DataFrameNoneException
from jobs.tasks.analytics.filter import Filter
from jobs.tasks.analytics.sql_exec import SqlExecute
from jobs.tasks.analytics.udf import Udf
from jobs.tasks.io.table import Table


class Similarity:
    """
    This class is responsible main business logic of the workflow, which is finding similar customers.
    """

    # constants
    FILTER_COLUMN = "customer"
    SOURCE_TABLE_NAME = "customers"
    PROCESSED_TABLE_NAME = "customers_processed"

    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    def find_similar_customers(self, customer_df, filter_value, top_n_records=10):
        """
        This method executes the logic of finding similar customers.

        :param customer_df: This is the source pyspark dataframe
        :param filter_value: This is the customer id which will be used for filtering
        :param top_n_records: This is the number similar customers required

        :return: dataframe with results in it
        """
        self.logger.info("# Initiating the workflow to find Similar customers for customer id: " + filter_value)

        if customer_df is None or filter_value is None:
            raise DataFrameNoneException("Please provide valid customer source and filter value")

        self.create_temp_table(customer_df, self.SOURCE_TABLE_NAME)
        attribute_values_set, filtered_record = self.get_filtered_record_values(customer_df, filter_value)

        attribute_attribute_keys = self.get_attribute_keys(filtered_record)
        customer_df_combined_attribute = self.get_customers_with_combined_df(attribute_attribute_keys)
        self.spark.catalog.uncacheTable(self.SOURCE_TABLE_NAME)  # Relinquishing resources
        self.logger.info("# Regsitering UDF with the new set of attribute values")
        Udf.register_udf(self.spark, attribute_values_set)
        self.create_temp_table(customer_df_combined_attribute, self.PROCESSED_TABLE_NAME)
        return self.get_similar_customers(top_n_records)

    def get_filtered_record_values(self, customer_df, filter_value):
        """
        This method takes the source dataframe, filters it and returns the attribute field values,
        which is crucial part of the program.

        :param customer_df: This is the source dataframe
        :param filter_value: This is the customer id
        :return: attributes_values and filtered_record
        """
        customer_filtered_df = self.filter_customer_df(customer_df, self.FILTER_COLUMN, filter_value)
        self.logger.info("# Extracting values from filtered dataframe")

        if customer_filtered_df is None or len(customer_filtered_df.take(1)) == 0:
            raise ValueError("No records found for the customer id : " + str(filter_value))

        filtered_record = customer_filtered_df.take(1)[0]
        return self.get_attribute_values(filtered_record), filtered_record

    def get_attribute_keys(self, record):
        """
        This method takes the filtered record and returns the comma separated list of attributes sub-fields.

        :param record: Filtered record
        :return: String of attributes column sub-fields
        """
        if record is None:
            raise ValueError("No record found to extract attributes from")
        record_list = list(str("attributes.`" + s + "`") for s in record.attributes.asDict().keys())
        return ",' ',".join(record_list)

    def filter_customer_df(self, customer_df, filter_column, filter_value):
        """
        This method calls the filter_dataframe method of the Filter service.

        :param customer_df: source dataframe
        :param filter_column: id column
        :param filter_value: customer id
        :return: filtered dataframe
        """
        self.logger.info("# Filtering source dataframe")
        return Filter().filter_dataframe(customer_df, filter_column, filter_value)

    def get_attribute_values(self, record):
        """
        This method takes the filtered record and returns attributes columns values in 'set',
        this is used to check against all the records inside source dataframe.

        :param record: Filtered record
        :return: set of attribute values
        """
        if record is None:
            raise ValueError("No record found to extract attributes from")
        return set(str(value) for value in record.attributes.asDict().values())

    def create_temp_table(self, customer_filtered_df, table_name):
        """
        This method calls the create_temp_table method of Table service.

        :param customer_filtered_df: filtered df
        :param table_name: temp table name
        """
        self.logger.info("# Creating temp view from source")
        Table(self.logger).create_temp_table(customer_filtered_df, table_name)

    def execute_sql(self, query):
        """
        Executes the given Spark-SQL query.

        :param query: Spark SQL query
        :return: resulting dataframe
        """
        return SqlExecute(self.spark, self.logger) \
            .execute(query)

    def get_similar_customers(self, limit):
        """
        Executes the query which gets the results
        :param limit: limit on number of reords
        :return: resulting dataframe
        """
        self.logger.info("# Getting final results")

        query = "SELECT customer, attributes, exists_in_set(combined_attributes) as num_similar_attributes " \
                "FROM " + self.PROCESSED_TABLE_NAME \
                + " ORDER BY num_similar_attributes " \
                  "DESC LIMIT " + str(limit)
        return self.execute_sql(query)

    def get_customers_with_combined_df(self, attribute_column_names):
        """
        Executes the query which gets the results with combined attributes.
        :param attribute_column_names: This is names of attributes fields
        :return: resulting dataframe
        """
        query = "SELECT *," \
                " concat(" \
                + attribute_column_names \
                + ") as combined_attributes " \
                  "FROM " + self.SOURCE_TABLE_NAME
        return self.execute_sql(query)

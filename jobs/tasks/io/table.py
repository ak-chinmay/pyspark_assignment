class Table:
    """
    This class is responsible for Creation of table
    """

    def __init__(self, logger):
        """
        This constructor initialises logger object.

        :param logger: Logger object
        """
        self.logger = logger

    def create_temp_table(self, df, table_name):
        """
        This method creates the temp view

        :param df: Dataframe
        :param table_name: tablename to be given
        """
        self.logger.info("# Creating a temp table " + table_name)
        df.createOrReplaceTempView(table_name)

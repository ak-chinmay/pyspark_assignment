class Filter:
    """
    This class is responsible for holding the Filter service logic
    """
    def filter_dataframe(self, df, filter_column, filter_value):
        """
        Filters the dataframe
        :param df: dataframe
        :param filter_column: filter column
        :param filter_value: filter value
        :return: resulting dataframe
        """
        return df.filter((df[filter_column] == filter_value))


if __name__ == '__main__':
    pass

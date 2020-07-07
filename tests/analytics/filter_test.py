import unittest

from jobs.tasks.analytics.filter import Filter
from tests.TestUtils import TestUtils


class FilterTest(unittest.TestCase):

    def test_filter_dataframe(self):
        spark = TestUtils.get_spark_instance()
        customer_df = TestUtils.read_file(spark)
        actual_count = Filter().filter_dataframe(customer_df, 'customer', 'customer-1').count()
        expected_count = 1
        self.assertEqual(expected_count, actual_count)

if __name__ == '__main__':
    unittest.main()
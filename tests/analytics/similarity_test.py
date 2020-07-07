import unittest

from pytest import fail

from jobs.exceptions import DataFrameNoneException
from jobs.tasks.analytics.similarity import Similarity
from jobs.util.process_log import ProcessLog
from tests.TestUtils import TestUtils


class SimilarityTest(unittest.TestCase):
    def setUp(self):
        self.spark = TestUtils.get_spark_instance()
        logger = ProcessLog().getLogger()
        self.similarity = Similarity(self.spark, logger)

    def test_find_similar_customers(self):
        try:
            customer_df = TestUtils.read_file(self.spark)
            result = self.similarity.find_similar_customers(customer_df, 'customer-1')

            actual_customer_id = result.take(1)[0].customer
            expected_customer_id = 'customer-5349'
            self.assertEqual(expected_customer_id, actual_customer_id)

            expected_count = 10
            actual_count = result.count()
            self.assertEqual(expected_count, actual_count)

        except Exception as e:
            fail(e.__str__())

    def test_find_similar_customers_none_df(self):
        self.assertRaises(DataFrameNoneException, self.similarity.find_similar_customers, None, "customer-1")

    def test_get_filtered_record_values(self):
        try:
            customer_df = TestUtils.read_file(self.spark)
            actual_attributes_values_set, actual_filtered_record = self.similarity \
                .get_filtered_record_values(customer_df, 'customer-1')
            expected_attributes_values_set = {'att-b-3', 'att-c-10', 'att-a-7', 'att-f-11', 'att-h-7', 'att-i-5',
                                              'att-j-14', 'att-e-15', 'att-g-2', 'att-d-10'}
            self.assertEqual(expected_attributes_values_set, actual_attributes_values_set)
        except Exception as e:
            fail(e.__str__())

    def test_get_filtered_record_values_non_matching(self):
        customer_df = TestUtils.read_file(self.spark)
        self.assertRaises(ValueError, self.similarity.get_filtered_record_values, customer_df, "NON_MATCHING_VALUE")

    def test_get_attribute_keys(self):
        try:
            customer_df = TestUtils.read_file(self.spark)
            attributes_values_set, filtered_record = self.similarity \
                .get_filtered_record_values(customer_df, 'customer-1')
            actual_attribute_keys = self.similarity.get_attribute_keys(filtered_record)
            expected_attribute_keys = "attributes.`att-a`,' ',attributes.`att-b`,' ',attributes.`att-c`,' ',attributes.`att-d`,' ',attributes.`att-e`,' ',attributes.`att-f`,' ',attributes.`att-g`,' ',attributes.`att-h`,' ',attributes.`att-i`,' ',attributes.`att-j`"
            self.assertEqual(expected_attribute_keys, actual_attribute_keys)
        except Exception as e:
            fail(e.__str__())


if __name__ == '__main__':
    unittest.main()

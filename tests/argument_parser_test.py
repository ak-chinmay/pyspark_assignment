import unittest

import sys

from jobs.argument_parser import ArgumentParser


class ArgumentParserTest(unittest.TestCase):
    argumentParser = None
    mainFile = "__main.py__"

    def setUp(self):
        self.argumentParser = ArgumentParser()

    def test_parse_runnAllTests(self):
        sys.argv = [self.mainFile, "--test"]

        customer_id, file_path, test = self.argumentParser.parse()
        self.assertTrue(test)

    def test_parse_target(self):
        expected_customer_id = "customer-1"
        sys.argv = [self.mainFile, "-c", expected_customer_id]
        actual_customer_id, file_path, test = self.argumentParser.parse()
        self.assertEqual(expected_customer_id, actual_customer_id)

    def test_parse_drug(self):
        expected_file_path = "/tmp/assignment.json"
        expected_customer_id = "customer-1"
        sys.argv = [self.mainFile, "-f", expected_file_path, "-c", expected_customer_id]
        actual_customer_id, actual_file_path, test = self.argumentParser.parse()
        self.assertEqual(expected_file_path, actual_file_path)


if __name__ == '__main__':
    unittest.main()

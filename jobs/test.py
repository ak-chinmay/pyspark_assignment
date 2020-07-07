from unittest import main
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

verbosity = 2
exit = False
print("\n# Running all the tests from the assignment!\n")
main(module='tests.argument_parser_test', exit=exit, verbosity=verbosity)
main(module='tests.analytics.filter_test', exit=exit, verbosity=verbosity)
main(module='tests.analytics.similarity_test', exit=exit, verbosity=verbosity)
import inspect, os, sys
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# from timeout_decorator import timeout
import unittest
from banking_system_impl import BankingSystemImpl
import pytest # type: ignore
import pytest_timeout # type: ignore


class SandboxTests(unittest.TestCase):
    """
    The test class below can be considered as a playground - feel free to modify it as you need, e.g.:
    - add your own custom tests
    - delete existing tests
    - modify test contents or expected output

    The results of tests from this file will always be at the beginning of the report generated by clicking the "Run" button.

    The results of these tests do not affect the final score (unless the project fails to build).
    """

    failureException = Exception


    @classmethod
    def setUp(cls):
        cls.system = BankingSystemImpl()

    @pytest.mark.timeout(0.4)
    def test_sample(self):
        self.assertTrue(self.system.create_account(1, 'account1'))
        self.assertTrue(self.system.create_account(2, 'account2'))
    def test_dep(self):
        self.assertEqual(self.system.deposit(3, 'account1', 2000), 2000)
        self.assertEqual(self.system.deposit(4, 'account2', 1000), 1000)
    def test_transfer(self):
        self.assertEqual(self.system.transfer(5, 'account1', 'account2', 500), 1500)


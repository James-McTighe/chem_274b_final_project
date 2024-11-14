from banking_system import BankingSystem
import sqlite3
from sqlite3 import OperationalError


class BankingSystemImpl(BankingSystem):

    def __init__(self, db_name):
        super().__init__(db_name)
        # TODO: implement
        pass

    # TODO: implement interface methods here


a = BankingSystemImpl('chem_274B_fp.db')
from banking_system import BankingSystem
from db_base_class import Query
import sqlite3

class BankingSystemImpl(BankingSystem, Query):

    def __init__(self):
        super().__init__()
        self.db_name = "chem_274B_fp.db"
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()

        with open("create.sql") as f:
            sql_script = f.read()
            self.cur.executescript(sql_script)
        
    
    def create_account(self, timestamp, account_id):
        
        self.connect()

        try:
            self.insert_user_data(account_id, timestamp, True, None, None, 0)
            return True
        except sqlite3.IntegrityError:
            self.close()
            return False
    
            
    def deposit(self, timestamp, account_id, amount, date_of_transaction, cash_back_date):
       
        if self.check_if_value_exists('user_data', 'account_id', account_id):
            old_balance = self.get_data_base_info("balances", "amount", account_id)
            new_balance = old_balance + amount
            self.update_account_balance(new_balance, timestamp, account_id)

            self.record_transaction(
                account_id,
                amount,
                date_of_transaction,
                "deposit",
                cash_back_date
            )

            return new_balance

        else:
            return None

    def transfer(self, timestamp, source_account_id, target_account_id, amount):
        valid_source = self.check_if_value_exists('user_data', 'account_id', source_account_id)
        valid_target = self.check_if_value_exists('user_data', 'account_id', target_account_id)

        if not valid_source or not valid_target:
            return None
        if source_account_id == target_account_id:
            return None

        self.connect()
        self.cur.execute(
            "SELECT account_balance "
            "FROM user_data "
            "WHERE account_id=?",
            (source_account_id,)
        )
        source_result = self.cur.fetchone()
        source_balance = source_result[0]

        if source_balance < amount:
            self.close()
            return None
        
        self.connect()
        self.cur.execute(
            "UPDATE user_data "
            f"SET account_balance=account_balance - {amount} "
            f"WHERE account_id='{source_account_id}';"
        )
        self.cur.execute(
            "UPDATE user_data "
            f"SET account_balance=account_balance + {amount} "
            f"WHERE account_id='{target_account_id}';"
        )

        self.conn.commit()

        self.cur.execute(
            "SELECT account_balance "
            "FROM user_data "
            "WHERE account_id=?", (source_account_id,)
        )
        source_result=self.cur.fetchone()
        source_balance=source_result[0]

        self.record_transaction(
            source_account_id,
            -amount,
            timestamp,
            "transfer",
            "None"
        )

        self.close()

        return source_balance


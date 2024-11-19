import sqlite3
from banking_system import BankingSystem
from db_base_class import Query

class BankingSystemImpl(BankingSystem, Query):

    def __init__(self):
        # super().__init__()
        self.db_name = "chem_274B_fp.db"
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        self.connect()
        with open("./db_scr/create.sql") as file:
            sql_script = file.read()
        self.cur.executescript(sql_script)
        self.close()
        
    
    def create_account(self, timestamp, account_id):
        
        self.connect()

        try:
            self.insert_user_data(account_id, timestamp, 1, 0, 0, 0)
            return True
        except sqlite3.IntegrityError:
            self.close()
            return False
    
            
    def deposit(self, timestamp, account_id, amount):
       
        if self.check_if_value_exists('user_data', 'account_id', account_id):
            old_balance = self.get_data_base_info("balances", "amount", account_id)
            new_balance = old_balance + amount
            self.update_account_balance(new_balance, timestamp, account_id)

            self.record_transaction(
                account_id,
                amount,
                "deposit",
            )

            return new_balance

        else:
            return None

    def transfer(self, timestamp, source_account_id, target_account_id, amount):
        if source_account_id == target_account_id:
            return None
        
        valid_source = self.check_if_value_exists('user_data', 'account_id', source_account_id)
        valid_target = self.check_if_value_exists('user_data', 'account_id', target_account_id)

        if not valid_source or not valid_target:
            return None

        source_result = self.get_data_base_info("balances", "amount", source_account_id)
        source_balance = source_result
        new_source_balance = source_balance - amount

        if source_balance < amount:
            return None
        
        self.update_account_balance(new_source_balance, timestamp, source_account_id)

        target_balance = self.get_data_base_info("balances", "amount", target_account_id)
        new_target_balance = target_balance + amount
        self.update_account_balance(new_target_balance, timestamp, target_account_id)
      
        self.record_transaction(
            source_account_id,
            -amount,
            timestamp,
            "transfer",
            "None"
        )

        return new_source_balance

a = BankingSystemImpl()

import os
import sqlite3
from banking_system import BankingSystem
from db_base_class import Query

class BankingSystemImpl(BankingSystem, Query):

    def __init__(self):
        # super().__init__()
        # Delete database if it exists already.
        if os.path.exists("chem_274B_fp.db"):
            os.remove("chem_274B_fp.db")
        
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

        if self.check_if_value_exists("user_data", "account_id", account_id):
            return False

        self.insert_user_data(account_id, timestamp, 1, 1, 1, 1)
        self.new_balance(account_id, 0, timestamp)
        return True
    
            
    def deposit(self, timestamp, account_id, amount):
       
        if self.check_if_value_exists('user_data', 'account_id', account_id):
            old_balance = self.get_account_balance(account_id)
            new_balance = old_balance + amount
            self.update_account_balance(new_balance, timestamp, account_id)
            self.record_transaction(
                account_id,
                amount,
                timestamp,
                "deposit",
            )
            print(new_balance)
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

        source_result = self.get_account_balance(source_account_id)
        source_balance = source_result
        new_source_balance = source_balance - amount

        if source_balance < amount:
            return None
        
        self.update_account_balance(new_source_balance, timestamp, source_account_id)

        target_balance = self.get_account_balance(target_account_id)
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
    
    def top_spenders(self, timestamp: int, n:int) -> list[str]:
        """
        Function to return the top n accounts based on outgoing transactions
        """
        # Create avariable which stores a tuple: (account id, sum(outgoing transactions)) 
        output = self.execute_script("top_spenders", (n,)) 

        # Perform list comprehension to extract tuple from the output
        return [f"{account_id}({int(total_out)})" for account_id, total_out in output]
    
# a = BankingSystemImpl()
# print(a.get_data_base_info("balances","amount","account1"))
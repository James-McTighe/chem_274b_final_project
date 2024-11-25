import os
import sqlite3
import banking_system
from banking_system import *

create_tables = """
BEGIN;
-- Drop tables if they exist
DROP TABLE IF EXISTS user_data;
DROP TABLE IF EXISTS balances;
DROP TABLE IF EXISTS transactions;

CREATE TABLE IF NOT EXISTS user_data (
    account_id VARCHAR(255),
    create_date TIMESTAMP,
    active BOOLEAN DEFAULT True,
    merge_id VARCHAR(255),
    merge_date TIMESTAMP,
    account_balance INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS balances (
    account_id VARCHAR(255),
    amount INT,
    account_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    account_id VARCHAR(255),
    amount INT,
    date_of_transaction TIMESTAMP,
    type_of_transaction VARCHAR(255),
    cashback_date TIMESTAMP
);
COMMIT;
"""

insert_user_data = """
INSERT INTO user_data
VALUES (
    ?, ?, ?, ?, ?, ?
);
"""

new_balance="""
INSERT INTO balances
VALUES (?, ?, ?);
"""

record_transaction="""
INSERT INTO transactions
VALUES (
    ?, ?, ?, ?, ?
);
"""

top_spenders="""
SELECT 
    D.account_id, 
    COALESCE(SUM(CASE
                    WHEN T.amount < 0 THEN -T.amount 
                    ELSE 0
                 END), 0) as total_outgoing_transactions
FROM user_data D LEFT JOIN transactions T
ON D.account_id = T.account_id
GROUP BY D.account_id
ORDER BY 
    total_outgoing_transactions DESC, 
    D.account_id ASC
LIMIT ?;
"""

update_account="""
UPDATE user_data
SET ?=?
WHERE account_id=?;
"""

update_balance="""
UPDATE balances
SET amount=?, account_date=?
WHERE account_id=?;
"""

class Query(ABC):

    def __init__(self):
        super().__init__()
        self.db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()

    # Methods for database management
    def connect(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def commit_and_close(self):
        self.conn.commit()
        self.close()

    def execute_script(self, script:str, parameters:tuple|None =None):
        """
        Executes a pre-written SQL script.
        
        """
        self.connect()
        

        if parameters:
            self.cur.execute(script, parameters)
        else:
            self.cur.execute(script)

        # Obatin the ouput from script execution
        output = self.cur.fetchall()
        self.commit_and_close()

        return output
        
    def check_if_value_exists(self, table_name:str, column_name:str, value) -> bool:
        self.connect()
        self.cur.execute(f"SELECT 1 FROM {table_name} WHERE {column_name}=?",(value,))
        result = self.cur.fetchone()
        self.close()
        return result is not None
    
    """
    The following functions are used to automate running scripts in the `db_scr` folder.
    These will update the database for the appropriate tables while only having to enter the input parameters
    """

    def insert_user_data(
        self,
        account_id,
        creation_date,
        active,
        merge_id,
        merge_date,
        account_balance):
        
        entered_data = (
            account_id,
            creation_date,
            active,
            merge_id,
            merge_date,
            account_balance
        )

        self.execute_script(insert_user_data,entered_data)

    def record_transaction(
        self,
        account_id,
        amount,
        date_of_transaction,
        type_of_transaction,
        cashback_date = None
    ):
        entered_data = (
            account_id,
            amount,
            date_of_transaction,
            type_of_transaction,
            cashback_date
        )
        
        self.execute_script(record_transaction,entered_data)

    def update_account_info(self, parameter, value, account_id):
        entered_data = (parameter, value, account_id)
        self.execute_script(update_account, entered_data)

    def new_balance(self, account_id, amount, timestamp):
        entered_data = (account_id, amount, timestamp)
        self.execute_script(new_balance, entered_data)

    def update_account_balance(self, amount, account_date, account_id):
        entered_data = (amount, account_date, account_id)
        self.execute_script(update_balance, entered_data)

    def get_account_balance(self, account_id):
        self.connect()
        self.cur.execute(
            f"SELECT amount FROM balances WHERE account_id='{account_id}';"
        )
        result = self.cur.fetchone()
        self.close()
        return result[0]



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
        self.cur.executescript(create_tables)
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
        output = self.execute_script(top_spenders, (n,)) 

        # Perform list comprehension to extract tuple from the output
        return [f"{account_id}({int(total_out)})" for account_id, total_out in output]
    
# a = BankingSystemImpl()
# print(a.get_data_base_info("balances","amount","account1"))
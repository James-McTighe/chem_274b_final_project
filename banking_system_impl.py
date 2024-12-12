import os
import sqlite3
import banking_system
from banking_system import *
import math

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

CREATE TABLE IF NOT EXISTS balance_history (
    account_id VARCHAR(255),
    amount INT,
    balance_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    account_id VARCHAR(255),
    amount INT,
    date_of_transaction TIMESTAMP,
    type_of_transaction VARCHAR(255),
    payment_number VARCHAR(255),
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
    ?, ?, ?, ?, ?, ?
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

update_balance="""
UPDATE balances
SET amount=?, account_date=?
WHERE account_id=?;
"""

record_balance="""
INSERT INTO balance_history
VALUES (?, ?, ?);
"""

check_cashbacks="""
SELECT SUM(amount)
FROM transactions 
WHERE account_id=? 
AND type_of_transaction='payment'
AND cashback_date <= ?
;
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
    
    def active(self, account_id):
        self.connect()
        try:
            active=self.cur.execute(f"SELECT active from user_data WHERE account_id='{account_id}'").fetchone()[0]
        except:
            return False
        return active
    
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
        payment_number = None,
        cashback_date = None
    ):
        entered_data = (
            account_id,
            amount,
            date_of_transaction,
            type_of_transaction,
            payment_number,
            cashback_date
        )
        
        self.execute_script(record_transaction,entered_data)

    def update_account_info(self, column, value, account_id):
        self.connect()
        entered_data = (value, account_id)
        update_account=f"""
        UPDATE user_data
        SET {column}=?
        WHERE account_id=?;
        """
        self.execute_script(update_account, entered_data)

    def new_balance(self, account_id, amount, timestamp):
        entered_data = (account_id, amount, timestamp)
        self.execute_script(new_balance, entered_data)

    def update_account_balance(self, amount, account_date, account_id):
        entered_data = (amount, account_date, account_id)
        self.execute_script(update_balance, entered_data)

    def record_balance(self, account_id, amount, timestamp):
        self.connect()
        entered_data = (account_id, amount, timestamp)
        self.execute_script(record_balance, entered_data)

    def get_account_balance(self, account_id):
        self.connect()
        self.cur.execute(
            f"SELECT amount FROM balances WHERE account_id='{account_id}';"
        )
        result = self.cur.fetchone()
        self.close()
        return result[0]

    def check_cashbacks(self, account_id, timestamp):
        try:
            self.connect()
            entered_data = (account_id, timestamp)
            self.cur.execute(check_cashbacks, entered_data)
            result=self.cur.fetchone()[0]
            if result is None:
                return 0
            else:
                return math.floor(-result*0.02)
        except:
            return 0


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
        self.record_balance(account_id, 0, timestamp)
        return True
    
            
    def deposit(self, timestamp, account_id, amount):
       
        if self.active(account_id) and self.check_if_value_exists('user_data', 'account_id', account_id):
            old_balance = self.get_account_balance(account_id)
            new_balance = old_balance + amount 
            self.update_account_balance(new_balance, timestamp, account_id)
            self.record_balance(account_id, new_balance, timestamp)
            self.record_transaction(
                account_id,
                amount,
                timestamp,
                "deposit",
            )
            return new_balance + self.check_cashbacks(account_id, timestamp)

        else:  
            return None
        

    def transfer(self, timestamp, source_account_id, target_account_id, amount):
        if source_account_id == target_account_id:
            return None
        
        valid_source = self.active(source_account_id) and self.check_if_value_exists('user_data', 'account_id', source_account_id)
        valid_target = self.active(target_account_id) and self.check_if_value_exists('user_data', 'account_id', target_account_id)

        if not valid_source or not valid_target:
            return None
        

        source_result = self.get_account_balance(source_account_id)
        source_balance = source_result
        new_source_balance = source_balance - amount

        if source_balance < amount:
            return None
        
        self.update_account_balance(new_source_balance, timestamp, source_account_id)
        self.record_balance(source_account_id, new_source_balance, timestamp)

        target_balance = self.get_account_balance(target_account_id)
        new_target_balance = target_balance + amount
        self.update_account_balance(new_target_balance, timestamp, target_account_id)
        self.record_balance(target_account_id, new_target_balance, timestamp)
      
        self.record_transaction(
            source_account_id,
            -amount,
            timestamp,
            "transfer",
            "None"
        )

        return new_source_balance + self.check_cashbacks(source_account_id, timestamp)
    
    def top_spenders(self, timestamp: int, n:int) -> list[str]:
        output = self.execute_script(top_spenders, (n,))
        print(f"Output from execute_script: {output}")

    # Check if output is empty or not
        if not output:
            print("No output received from execute_script.")
    
    # Perform list comprehension to extract tuple from the output
        result = [f"{account_id}({int(total_out)})" for account_id, total_out in output]
        print(f"Formatted result: {result}")
    
        return result
        """
        Function to return the top n accounts based on outgoing transactions
        
        # Create avariable which stores a tuple: (account id, sum(outgoing transactions)) 
        output = self.execute_script(top_spenders, (n,)) 

        # Perform list comprehension to extract tuple from the output
        return [f"{account_id}({int(total_out)})" for account_id, total_out in output]
        """
    
    def pay(self, timestamp:int, account_id:str, amount:int) -> str|None:

        self.connect()
        if self.active(account_id) and self.check_if_value_exists('user_data', 'account_id', account_id):
            new_balance = self.get_account_balance(account_id) - amount + self.check_cashbacks(account_id, timestamp)
            actual_balance = self.get_account_balance(account_id) - amount
            if new_balance >= 0:
                self.update_account_balance(actual_balance, timestamp, account_id)
                cashback_date = timestamp + 86400000
                self.connect()
                count=self.cur.execute(f"SELECT COUNT(*) from transactions WHERE type_of_transaction = 'payment' ").fetchone()[0]
                payment_number="payment" + str(count+1)
                self.record_transaction(account_id, -amount, timestamp, "payment", payment_number, cashback_date)
                self.record_balance(account_id, actual_balance, timestamp)
                return payment_number
            else:
                return None
        else:
            return None
        
    def get_payment_status(self, timestamp: int, account_id: str, payment: str) -> str | None:
        self.connect()
        if self.active(account_id) and self.check_if_value_exists('user_data', 'account_id', account_id):
            try:
                self.connect()
                payment_date = self.cur.execute(f"SELECT cashback_date from transactions WHERE payment_number = '{payment}' AND account_id = '{account_id}' ;").fetchone()[0]
                if payment_date > timestamp:
                    return "IN_PROGRESS"
                else:
                    return "CASHBACK_RECEIVED"
            except:
                return None
        else:
            return None
    
    def merge_accounts(self, timestamp: int, account_id_1: str, account_id_2: str) -> bool:
        valid_source = self.check_if_value_exists('user_data', 'account_id', account_id_1)
        valid_target = self.check_if_value_exists('user_data', 'account_id', account_id_2)

        if not valid_source or not valid_target:
            return False
        if account_id_1 == account_id_2:
            return False
        
    

        self.connect()
        self.update_account_info(column="merge_id",value=account_id_1,account_id=account_id_2)
        self.update_account_info("merge_id",account_id_2,account_id_1)
        self.update_account_info("active",0,account_id_2)


        merge_balance = self.get_account_balance(account_id_1) + self.get_account_balance(account_id_2)
        self.update_account_balance(merge_balance, timestamp, account_id_1)
        self.record_balance(account_id_1, merge_balance, timestamp)
        return True
    #all pending cashback refunds for account 2 need to be reassigned. look at the pending cashbacks, change these associated account IDs to the new merge account. 
    # merge transaction history, then merge pending cashback with extend function
    #one line of code for transaction history, use extend function 
    #add delete account 2 delete function python (del)
    

    def get_balance(self, timestamp: int, account_id: str, time_at: int) -> int | None:
        self.connect()

        #Account hasn't been created yet
        try:
            cdate=self.cur.execute(f"SELECT create_date from user_data WHERE account_id='{account_id}'").fetchone()[0]
        except:
            return None

        mdate=self.cur.execute(f"SELECT merge_date from user_data WHERE account_id='{account_id}'").fetchone()[0]

        active=self.cur.execute(f"SELECT active from user_data WHERE account_id='{account_id}'").fetchone()[0]

        if active and mdate:
            merge_id=self.cur.execute(f"SELECT merge_id from user_data WHERE account_id = '{account_id}' ").fetchone()[0]

        if time_at < cdate:
            return None

        transaction=self.cur.execute(f"SELECT * from transactions WHERE account_id='{account_id}' AND date_of_transaction={time_at} ").fetchone()

        balance=self.cur.execute(f"SELECT MAX(balance_date), amount from balance_history WHERE account_id='{account_id}' AND balance_date <= {time_at}").fetchone()[1]
        return balance + self.check_cashbacks(account_id, time_at)

        if transaction==None:
            balance=self.cur.execute(f"SELECT MAX(account_date), amount from balances WHERE account_id='{account_id}' AND account_date >= {time_at}").fetchone()[1]

        #Check if account is disabled 
        #might be able to return none here 
        active=self.cur.execute(f"SELECT active from user_data WHERE account_id='{account_id}'")
        if active.fetchone()[0] == 0:
            merge_id=self.cur.execute(f"SELECT merge_id from user_data WHERE account_id = '{account_id}' ").fetchone()[0]

            #Check if transactions occur

            balance_date=self.cur.execute(f"SELECT MAX(create_date) from user_data WHERE account_id='{merge_id}' AND create_date >= {time_at}").fetchone()[0]

        self.close()
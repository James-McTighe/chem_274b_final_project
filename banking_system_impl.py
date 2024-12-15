import os
import sqlite3
import banking_system
from banking_system import *
import math


"""
SQL Statements Documentation

creates tables: This SQL script creates the necessary tables for the banking system. 
insert_user_data: Inserts a new user into the user_data table. The values to be inserted are provided as parameters.
new_balance: Inserts a new balance record into the balances table. The values to be inserted are provided as parameters.
record_transaction: Records a transaction in the transactions table. The values to be inserted are provided as parameters.
top_spenders: Retrieves the top spenders based on the outgoing transactions. The result is organized by the total outgoing transactions in descending order.
update_balance: Updates the balance for a specific account in the balances table.
check_cashbacks: Checks the total cashbacks for a specific account up to a given date.
merge_cashbacks: Retrieves the cashbacks for a specific account up to a given date.
update_transaction_id: Updates the account ID for transactions in the transactions table.
record_balance: Records a balance history entry in the balance_history table.
delete_account: deletes a user account from the user_data table
delete_balance: deletes a balance record for the specific account from the balances table.
add_merge_date: Updates the merge date for an account in the balance_history table.

"""

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
    payment_number VARCHAR(255),
    cashback_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS balance_history (
    account_id VARCHAR(255),
    amount INT,
    balance_date TIMESTAMP,
    merge_date TIMESTAMP
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

check_cashbacks="""
SELECT SUM(amount)
FROM transactions 
WHERE account_id=? 
AND type_of_transaction='payment'
AND cashback_date <= ?;
"""

merge_cashbacks="""
SELECT amount
FROM transactions 
WHERE account_id=? 
AND type_of_transaction='payment'
AND cashback_date <= ?;
"""

update_transaction_id="""
UPDATE transactions
SET account_id=?
WHERE account_id=?;
"""

record_balance="""
INSERT INTO balance_history
VALUES (?, ?, ?, ?);
"""

delete_account="""
DELETE FROM user_data WHERE account_id =?
"""

delete_balance="""
DELETE FROM balances WHERE account_id =?
"""

add_merge_date="""
UPDATE balance_history
SET merge_date=?
WHERE account_id=?
"""

class Query(ABC):
    """
A base class for database queries.

Attributes:
        db_name: The name of the database.
        conn: The connection to the database.
        cur: The cursor to the database.

Methods:
        connect: Connects to the database.
        close: Closes the connection to the database.
        commit_and_close: Commits the changes and closes the connection to the database.
        execute_script: Executes a SQL script.
        check_if_value_exists: Checks if a value exists in a table.
        active: Checks if an account is active.
    """
    def __init__(self):
        """
        Closes the database connection and cursor.

        This method checks if the cursor is open and closes it if it is. 
        It also closes the database connection if it is open. 
        """
        super().__init__()
        self.db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()

    # Methods for database management
    def connect(self):
        """
        Establishes a connection to the SQLite database.

        This method creates a new connection to the database and initializes the cursor for executing SQL commands.
        Its called whenever a new connection is needed.
        """
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()

    def close(self):
        """
        Closes the database connection and cursor.

        This method checks if the cursor is open and closes it if it is. 
        It also closes the database connection if it is open. 
        """
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def commit_and_close(self):
        """
        Commits the current transaction and closes the database connection.

        This method saves any changes made during the current transaction to the database 
        and then calls the `close` method to release resources.
        It should be used when you want to ensure that all changes are saved before closing the connection.
        """
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
        """
        Checks if a specific value exists in a given table and column.

        This method connects to the database, executes a SELECT query to check for the existence
        of the specified value in the specified column of the specified table, and returns a boolean
        indicating whether the value exists.

        Args:
            table_name (str): The name of the table to check.
            column_name (str): The name of the column to check.
            value: The value to check for in the specified column.

        Returns:
            bool: True if the value exists in the specified column of the table, False otherwise.
            """
        self.connect()
        self.cur.execute(f"SELECT 1 FROM {table_name} WHERE {column_name}=?",(value,))
        result = self.cur.fetchone()
        self.close()
        return result is not None
    
    def active(self, account_id):
        """
        Checks if an account is active based on the account ID.

        This method connects to the database and executes a SELECT query to retrieve the account
        information for the specified account ID. If the account exists, it returns the account
        information; otherwise, it returns False.

        Args:
            account_id: The unique identifier for the account to check.

        Returns:
            The account information if the account is active; otherwise, False.
        """
        self.connect()
        try:
            active=self.cur.execute(f"SELECT * from user_data WHERE account_id='{account_id}'")
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
        """
        insert_user_data: Inserts a new user into the database.
        """
        
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
        """
        Records a transaction in the database.
        """
        
        entered_data = (
            account_id,
            amount,
            date_of_transaction,
            type_of_transaction,
            payment_number,
            cashback_date
        )
        
        self.execute_script(record_transaction,entered_data)

    def record_balance(self, account_id, amount, timestamp, merge_date):
        """
        Records a balance in the database.
        """
        self.connect()
        entered_data = (account_id, amount, timestamp, merge_date)
        self.execute_script(record_balance, entered_data)


    def update_account_info(self, column, value, account_id):
        """
         Updates an account's information in the database.
        """
        self.connect()
        entered_data = (value, account_id)
        update_account=f"""
        UPDATE user_data
        SET {column}=?
        WHERE account_id=?;
        """
        self.execute_script(update_account, entered_data)

    def new_balance(self, account_id, amount, timestamp):
        """
         Inserts a new balance into the database.
        """

        entered_data = (account_id, amount, timestamp)
        self.execute_script(new_balance, entered_data)

    def update_account_balance(self, amount, account_date, account_id):
        """
        Updates an account's balance in the database.
        """
        entered_data = (amount, account_date, account_id)
        self.execute_script(update_balance, entered_data)

    def update_transaction_id(self, account_id_1, account_id_2):
        """
        Updates a transaction's ID in the database.
        """
        entered_data = (account_id_1, account_id_2)
        self.execute_script(update_transaction_id, entered_data)

    def get_account_balance(self, account_id):
        """
        Retrieves an account's balance from the database.
        """
        self.connect()
        self.cur.execute(
            f"SELECT amount FROM balances WHERE account_id='{account_id}';"
        )
        result = self.cur.fetchone()
        self.close()
        return result[0]
    
    def delete_account(self, account_id):
        """
        Deletes an account from the database.
        """
        entered_data=(account_id,)
        self.execute_script(delete_account, entered_data)
        self.execute_script(delete_balance, entered_data)

    def add_merge_date(self, timestamp, account_id):
        """
        Adds a merge date to an account in the database.
        """
        
        entered_data=(timestamp, account_id)
        self.execute_script(add_merge_date, entered_data)

    def check_cashbacks(self, account_id, timestamp):
        """
        Checks if an account has any cashbacks.
        """
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
        
    def merge_cashbacks(self,account_id, timestamp):
        """
        Merges cashbacks for an account.
        """
        try:
            self.connect()
            entered_data = (account_id, timestamp)
            self.cur.execute(merge_cashbacks, entered_data)
            result=self.cur.fetchall()
            if result is None:
                return 0
            else:
                sum = 0
                for i in result:
                    sum += math.floor(-i[0]*0.02)
                return sum
        except:
            return 0
        
    def check_transactions(self,account_id, timestamp):
        """
        Checks if an account has any transactions.
        """
        try:
            self.connect()
            entered_data = (account_id, timestamp)
            self.cur.execute(merge_cashbacks, entered_data)
            result=self.cur.fetchall()
            return len(result)
        except:
            return 0


class BankingSystemImpl(BankingSystem, Query):
    """
    A concrete implementation of a banking system that manages user accounts, transactions, and balances.

    This class includes methods for creating accounts, depositing funds, transferring money, 
    checking account status, and managing transactions. It uses the SQLite database to store 
    user data and transaction history.

    It inherits from:
        Query: A class for managing database queries and connections.

    Attributes:
        db_name (str): The name of the SQLite database file.
        conn (sqlite3.Connection): The connection object for the SQLite database.
        cur (sqlite3.Cursor): The cursor object for executing SQL commands.
    """

    def __init__(self):
        """
        Initializes a new instance of the BankingSystemImpl class.

        This constructor checks if the database file already exists. If it does, it deletes the 
        existing database file to start fresh. It then establishes a connection to the new 
        database and creates the necessary tables for the banking system.
        """
        # super().__init__()
        # Delete database if it exists already.
        if os.path.exists("chem_274B_fp.db"):
            os.remove("chem_274B_fp.db")
        
        self.db_name = "chem_274B_fp.db"
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        """
        Creates the necessary tables in the SQLite database.

        This method executes a predefined SQL script to create the tables required for 
        storing user data, balances, transactions, and balance history. It should be called 
        during initialization to set up the database structure.
        """
        self.connect()
        self.cur.executescript(create_tables)
        self.close()
        
    
    def create_account(self, timestamp, account_id):
        """
        Creates a new user account in the banking system.

        This method checks if an account with the given account ID already exists. If it does 
        not exist, it inserts a new user record and initializes the account balance.

        Args:
            timestamp (int): The timestamp of when the account is created.
            account_id (str): The unique identifier for the new account.

        Returns:
            bool: True if the account was successfully created, False if the account already exists.
        """
        
        self.connect()

        if self.check_if_value_exists("user_data", "account_id", account_id):
            return False

        self.insert_user_data(account_id, timestamp, 1, 1, 1, 1)
        self.new_balance(account_id, 0, timestamp)
        self.record_balance(account_id, 0, timestamp,None)
        return True
    
            
    def deposit(self, timestamp, account_id, amount):
        """
        Deposits a specified amount into the user's account.

        This method checks to make sure the account is active and it exists. If valid, it updates the account 
        balance and records the transaction.

        Args:
            timestamp (int): The timestamp of the deposit.
            account_id (str): The unique identifier for the account.
            amount (int): The amount to deposit.

        Returns:
            int | None: The new balance after the deposit if successful, None otherwise.
        """
       
        if self.active(account_id) and self.check_if_value_exists('user_data', 'account_id', account_id):
            old_balance = self.get_account_balance(account_id)
            new_balance = old_balance + amount
            self.update_account_balance(new_balance, timestamp, account_id)
            self.record_transaction(
                account_id,
                amount,
                timestamp,
                "deposit",
            )
            self.record_balance(account_id, new_balance, timestamp, None)
            return new_balance + self.check_cashbacks(account_id, timestamp)

        else:  
            return None
        

    def transfer(self, timestamp, source_account_id, target_account_id, amount):
        """
        Transfers a specified amount from one account to another.

        This method checks if both accounts are valid and active. If valid, it updates the balances 
        of both accounts and records the transaction.

        Args:
            timestamp (int): The timestamp of the transfer.
            source_account_id (str): The unique identifier for the source account.
            target_account_id (str): The unique identifier for the target account.
            amount (int): The amount to transfer.

        Returns:
            int | None: The new balance of the source account after the transfer if successful, None otherwise.
        """
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
        self.record_balance(source_account_id, new_source_balance, timestamp, None)

        target_balance = self.get_account_balance(target_account_id)
        new_target_balance = target_balance + amount
        self.update_account_balance(new_target_balance, timestamp, target_account_id)
        self.record_balance(target_account_id, new_target_balance, timestamp, None)
      
        self.record_transaction(
            source_account_id,
            -amount,
            timestamp,
            "transfer",
            "None"
        )

        return new_source_balance + self.check_cashbacks(source_account_id, timestamp)
    
    def top_spenders(self, timestamp: int, n:int) -> list[str]:
        """
    Retrieves the top n accounts based on outgoing transactions.

    This method executes a SQL query to find the accounts with the highest total outgoing 
    transactions. It returns a list of strings, that each represent an account ID and the 
    total outgoing amount in the format "account_id(total_out)".

    Args:
        timestamp (int): The timestamp to consider for the transactions.
        n (int): The number of top spenders to return.

    Returns:
        list[str]: A list of strings representing the top n accounts and their total outgoing 
        transaction amounts.
        """
        # Create avariable which stores a tuple: (account id, sum(outgoing transactions)) 
        output = self.execute_script(top_spenders, (n,)) 

        # Perform list comprehension to extract tuple from the output
        return [f"{account_id}({int(total_out)})" for account_id, total_out in output]
    
    def pay(self, timestamp:int, account_id:str, amount:int) -> str|None:
        """
    Processes a payment from the specified account.

    This method checks if the account is active and exists. If its valid, it calculates the new 
    balance after deducting the payment amount and updates the account balance. It also 
    records the transaction and returns the payment number.

    Args:
        timestamp (int): The timestamp of the payment.
        account_id (str): The unique identifier for the account making the payment.
        amount (int): The amount to be paid.

    Returns:
        str | None: The payment number if the payment is successful, None if the payment 
        cannot be processed (e.g., insufficient funds or inactive account).
        """

        self.connect()
        if self.active(account_id) and self.check_if_value_exists('user_data', 'account_id', account_id):
            new_balance = self.get_account_balance(account_id) + self.check_cashbacks(account_id, timestamp) - amount
            actual_balance = self.get_account_balance(account_id) - amount
            if new_balance >= 0:
                self.update_account_balance(new_balance, timestamp, account_id)
                cashback_date = timestamp + 86400000
                self.connect()
                count=self.cur.execute(f"SELECT COUNT(*) from transactions WHERE type_of_transaction = 'payment' ").fetchone()[0]
                payment_number="payment" + str(count+1)
                self.record_transaction(account_id, -amount, timestamp, "payment", payment_number, cashback_date)
                self.record_balance(account_id, actual_balance, timestamp, None)
                return payment_number
            else:
                return None
        else:
            return None
        
    def get_payment_status(self, timestamp: int, account_id: str, payment: str) -> str | None:
        """
    This method retrieves the status of a specific payment.

    Checks if the account is active and exists. If its valid, it queries the database 
    to find the cashback date for the specified payment number. It returns the payment status 
    based on whether the cashback date is in the future or has already occurred.

    Args:
        timestamp (int): The current timestamp to compare against the payment date.
        account_id (str): The unique identifier for the account associated with the payment.
        payment (str): The payment number to check the status of.

    Returns:
        str | None: "IN_PROGRESS" if the cashback date is in the future, "CASHBACK_RECEIVED" 
        if the cashback has been received, or None if the account is inactive or does not exist.
    """
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
        """
    Merges two user accounts into one.

    This method checks if both accounts are valid and exist. If both accounts are valid and 
    not the same, it combines their balances, then deletes the second account, and updates the 
    transaction records to reflect the merge.

    Args:
        timestamp (int): The timestamp of the merge operation.
        account_id_1 (str): The unique identifier for the first account (the one to keep).
        account_id_2 (str): The unique identifier for the second account (the one to be deleted).

    Returns:
        bool: True if the accounts were successfully merged, False if either account is invalid 
        or if both account IDs are the same.
    """
        valid_source = self.check_if_value_exists('user_data', 'account_id', account_id_1)
        valid_target = self.check_if_value_exists('user_data', 'account_id', account_id_2)

        if not valid_source or not valid_target:
            return False
        if account_id_1 == account_id_2:
            return False

        self.connect()
        #self.update_account_info(column="merge_id",value=account_id_2,account_id=account_id_2)
        #self.update_account_info("merge_id",account_id_1,account_id_1)
        #self.update_account_info("active",0,account_id_2)


        merge_balance = self.get_account_balance(account_id_1) + self.get_account_balance(account_id_2)
        self.update_account_balance(merge_balance, timestamp, account_id_1)
        self.delete_account(account_id_2)
        self.update_transaction_id(account_id_1, account_id_2)
        self.record_balance(account_id_1, merge_balance, timestamp, None)
        self.add_merge_date(timestamp, account_id_2)
        """
        self.cur.execute(
            "UPDATE user_data "
            f"SET merge_id='{account_id_1}' "
            f"WHERE account_id='{account_id_2}';"
        )
        self.cur.execute(
            "UPDATE user_data "
            f"SET active=0 "
            f"WHERE account_id='{account_id_2}';"
        )
        self.cur.execute(
            "UPDATE user_data "
            f"SET merge_id= '{account_id_1}' "
            f"WHERE account_id='{account_id_1}';"
        )
        self.cur.execute(
            "UPDATE transactions "
            f"SET account_id= '{account_id_1}' "
            f"WHERE account_id='{account_id_2}';"
        )
        """
        return True
    
    def get_balance(self, timestamp: int, account_id: str, time_at: int) -> int | None:
        """
    Retrieves the balance of a specified account at a given time.

    This method checks the account's balance history and calculates the balance at the specified 
    timestamp. It considers any cashbacks and merges that may have occurred.

    Args:
        timestamp (int): The current timestamp to compare against.
        account_id (str): The unique identifier for the account whose balance is being retrieved.
        time_at (int): The specific timestamp for which the balance is requested.

    Returns:
        int | None: The balance of the account at the specified time if it exists, None if the 
        account has not been created or if the balance cannot be determined.
        """
        self.connect()
        
        #Account hasn't been created yet
        creation_date=self.cur.execute(f"SELECT MIN(balance_date) from balance_history WHERE account_id='{account_id}'").fetchone()[0]
        if creation_date is None:
            return None
        
        try:
            merge_date_value=self.cur.execute(f"SELECT merge_date from balance_history WHERE account_id='{account_id}'AND merge_date IS NOT NULL").fetchone()[0]
        except:
            merge_date_value=None

        maxdate=self.cur.execute(f"SELECT MAX(balance_date) from balance_history WHERE account_id='{account_id}' ").fetchone()[0]

        if merge_date_value != None and merge_date_value <= time_at and merge_date_value >= maxdate:
            return None

        #active=self.cur.execute(f"SELECT active from user_data WHERE account_id='{account_id}'").fetchone()[0]

        #if active and merge_date_value:
        #merge_id=self.cur.execute(f"SELECT merge_id from user_data WHERE account_id = '{account_id}' ").fetchone()[0]  

        if time_at < creation_date:
            return None
        
        #if timestamp==time_at:
            return self.get_account_balance(account_id)+self.check_cashbacks(account_id, time_at)
        
        balance=self.cur.execute(f"SELECT MAX(balance_date), amount from balance_history WHERE account_id='{account_id}' AND balance_date <= {time_at}").fetchone()[1]
        l = self.check_transactions(account_id, time_at)

        if l >=9:
            return balance + self.merge_cashbacks(account_id, time_at)
        
        else:
            return balance + self.check_cashbacks(account_id, time_at)
        
        
        transaction=self.cur.execute(f"SELECT * from transactions WHERE account_id='{account_id}' AND date_of_transaction={time_at} ").fetchone()

        if transaction != None:
            balance=self.cur.execute(f"SELECT MAX(account_date), amount from balances WHERE account_id='{account_id}' AND account_date >= {time_at}").fetchone()[1]
            return balance

        if transaction==None:
            balance=self.cur.execute(f"SELECT MAX(account_date), amount from balances WHERE account_id='{account_id}' AND account_date >= {time_at}").fetchone()[1]
        
        #Check if account is disabled
        active=self.cur.execute(f"SELECT active from user_data WHERE account_id='{account_id}'")
        if active.fetchone()[0] == 0:
            merge_id=self.cur.execute(f"SELECT merge_id from user_data WHERE account_id = '{account_id}' ").fetchone()[0]

            #Check if transactions occur

            balance_date=self.cur.execute(f"SELECT MAX(create_date) from user_data WHERE account_id='{merge_id}' AND create_date >= {time_at}").fetchone()[0]

        self.close()
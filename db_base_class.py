import sqlite3
from abc import ABC

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
        
        with open(f"./db_scr/{script}.sql",'r') as f:
            sql_script = f.read()

        if parameters:
            self.cur.execute(sql_script, parameters)
        else:
            self.cur.execute(sql_script)

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

        self.execute_script("insert_user_data",entered_data)

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
        
        self.execute_script("record_transaction",entered_data)

    def update_account_info(self, parameter, value, account_id):
        entered_data = (parameter, value, account_id)
        self.execute_script("update_account", entered_data)

    def new_balance(self, account_id, amount, timestamp):
        entered_data = (account_id, amount, timestamp)
        self.execute_script("new_balance", entered_data)

    def update_account_balance(self, amount, account_date, account_id):
        entered_data = (amount, account_date, account_id)
        self.execute_script("update_balance", entered_data)

    def get_account_balance(self, account_id):
        self.connect()
        self.cur.execute(
            f"SELECT amount FROM balances WHERE account_id='{account_id}';"
        )
        result = self.cur.fetchone()
        self.close()
        return result[0]

    
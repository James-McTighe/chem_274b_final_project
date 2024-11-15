from banking_system import BankingSystem
import sqlite3
from sqlite3 import OperationalError


class BankingSystemImpl(BankingSystem):

    def __init__(self, db_name):
        super().__init__()
        self.db_name = db_name
        self.conn = None

        
    # Methods for database management
    def connect(self):
        self.conn = sqlite3.connect(self.db_name)

    def close(self):
        if self.conn:
            self.conn.close()

    def execute_query(self, query:str, params:str=None):
        """Executes a SQL query passed in as an argument
        Optional parameters may be included in the function if performed over several values
        Returns a cursor.  Note that the cursor and connection are not closed within this function
        
        Parameters
        ----------
        Query : string
        params : string (optional)
        
        Returns
        --------
        SQLite cursor
        """
        if not self.conn:
            self.connect()

        cur = self.conn.cursor()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)

        return cur.fetchall()
    
    def execute_script(self, script:str):
        self.connect()
        cur = self.conn.cursor()

        with open(f"{script}") as f:
            sql_script = f.read()

        cur.executescript(sql_script)
        cur.rowcount

        cur.close()
        self.close()

    def create_table(self):
        self.execute_script("create.sql")

    def create_account(self, timestamp, account_id):
        self.create_table()

        self.connect()
        cur = self.conn.cursor()

        self.execute_query("INSERT INTO user_data(account_id, create_date, active) "
                           f"VALUES('{account_id}', '{timestamp}', TRUE);")
        
        cur.execute("SELECT * FROM user_data;")
        for row in cur:
            print(row)

    def bool_test(self):
        self.connect()
        cur = self.conn.cursor()

        cur.execute("SELECT EXISTS (SELECT 1 FROM user_data WHERE account_id = 'Barbara');")        
        # return super().create_account(timestamp, account_id)
        for x in cur:
            if x[0] == 0:
                print("doesn't exist")

    


a = BankingSystemImpl('chem_274B_fp.db')
a.create_account(4, "Barbara")

a.bool_test()
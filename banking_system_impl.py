from banking_system import BankingSystem
import sqlite3

class BankingSystemImpl(BankingSystem):

    def __init__(self):
        super().__init__()
        self.db_name = "chem_274B_fp.db"
        self.conn = None
        self.cur = None
        
    # Methods for database management
    def connect(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()

    def close(self):
        if self.conn:
            self.cur.close()
            self.conn.close()

    def commit_and_close(self):
        self.conn.commit()
        self.close()

    def execute_script(self, script:str):
        """
        Executes a pre-written SQL script.
        
        """
        self.connect()
        
        with open(f"{script}") as f:
            sql_script = f.read()

        self.cur.executescript(sql_script)
        self.conn.commit()

        self.close()

    def create_table(self):
        self.execute_script("create.sql")

    def check_if_value_exists(self, table_name:str, column_name:str, value) -> bool:
        self.connect()
        self.cur.execute(f"SELECT 1 FROM {table_name} WHERE {column_name}=?",(value,))
        result = self.cur.fetchone()
        self.close()
        return result is not None

    def create_account(self, timestamp, account_id):
        self.create_table()

        self.connect()

        try:
            self.cur.execute("INSERT INTO user_data(create_date, account_id) VALUES(?,?)",(timestamp, account_id))
            self.commit_and_close()
            return True
        except sqlite3.IntegrityError:
            self.close()
            return False
        
    def deposit(self, timestamp, account_id, amount):
        self.connect()

        if not self.cur.execute(f"SELECT account_id FROM user_data WHERE account_id=?;", (account_id,)):
            self.close()
            return None
        else:
            self.cur.execute(
                "UPDATE user_data "
                f"SET account_balance=account_balance+{amount} "
                f"WHERE account_id='{account_id}';"
            )
            self.conn.commit()

        self.cur.execute(f"SELECT account_balance FROM user_data WHERE account_id='{account_id}'")
        result = self.cur.fetchone()
        updated_balance = result[0]
        self.close()

        return updated_balance
    
    def transfer(self, timestamp, source_account_id, target_account_id, amount):
        return super().transfer(timestamp, source_account_id, target_account_id, amount)



db = BankingSystemImpl()
print(db.check_if_value_exists('user_data','account_id','sara'))


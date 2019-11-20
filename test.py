import sqlite3
import re
class Db():
    def __init__ (self, path):
        self.path = path

    def execute(self, command):
        def none_to_null(command):
            return re.sub(r", ?(None) ?,", ", NULL,", command)
        command = none_to_null(command)
        self.db_conn = sqlite3.connect(self.path)
        self.db_crsr = self.db_conn.cursor()
        try:
            self.db_crsr.execute(command)
        except Exception as e:
            print(e)
            raise

    def get(self, command):
        self.execute(command)
        result = self.db_crsr.fetchall()
        return result
    
    def set(self, command):
        self.execute(command)
        self.db_conn.commit()
        self.db_conn.close()

    @property
    def tables(self):
        tables = self.get("SELECT name FROM sqlite_master WHERE type='table';")
        return list(map(lambda table: table[0], tables))

class Table():
    def __init__ (self, name, db):
        self.name = name
        self.db = db

    @property
    def columns(self):
        columns = self.db.get("SELECT name FROM pragma_table_info('{}')".format(self.name))
        return list(map(lambda column: column[0], columns)) 

    @property
    def npk_columns(self):
        columns = self.db.get("SELECT name FROM pragma_table_info('{}') WHERE pk=0".format(self.name))
        return list(map(lambda column: column[0], columns)) 

    @property
    def rows(self):
        rows = self.db.get("SELECT * FROM '{}'".format(self.name))
        return rows

    @property
    def primary_key(self):
        pk = self.db.get("SELECT name FROM pragma_table_info('{}') WHERE pk=1".format(self.name))
        return pk[0][0]

if __name__ == "__main__":
    db = Db("Northwind.db")
    def tables_with_col(col, db):
        return (list(map(lambda table: col in table, db.tables)))
    for t in db.tables:
        table = Table(t, db)
        cols=table.columns
        cols.remove(str(table.primary_key))
        print(table.name)
        print(cols)

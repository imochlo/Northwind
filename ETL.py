import sqlite3
import sys

class Db():
    def __init__ (self, path):
        self.dbPath = path

    def get(self, text_command):
        db_conn = sqlite3.connect(self.dbPath)
        db_crsr = db_conn.cursor()
        db_crsr.execute(text_command)
        return db_crsr.fetchall()
    
    def set(self, text_command):
        db_conn = sqlite3.connect(self.dbPath)
        db_crsr = db_conn.cursor()
        db_crsr.execute(text_command)
        db_conn.commit()
        db_conn.close()

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
        columns = db.get("PRAGMA table_info('{}')".format(self.name))
        return list(map(lambda column: column[1], columns)) 

if __name__ == "__main__":
    path = sys.argv[1]
    db = Db(path)
    print(db.tables)
    table1 = Table(db.tables[0], db)
    print(table1.columns)

import sqlite3
import json
import os.path
import sys
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
        self.db_crsr.execute(command)

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
        columns = self.db.get("PRAGMA table_info('{}')".format(self.name))
        return list(map(lambda column: column[1], columns)) 

    @property
    def rows(self):
        rows = self.db.get("SELECT * FROM '{}'".format(self.name))
        return rows

if __name__ == "__main__":
    source_path = sys.argv[1]
    source_db = Db(source_path)
    meta = []
    for t in source_db.tables:
        table = Table(t, source_db) 
        data = []
        for col in table.columns:
            data.append({col:True})
        meta.append({table.name:data})

    with open("Northwind_Meta.json", "w") as f:
        f.write(json.dumps(meta))

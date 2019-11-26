import sqlite3
import sys
import re

class Db():
    def __init__ (self, path):
        self.dbPath = path

    def execute(self, command):
        def none_to_null(command):
            return re.sub(r", ?(None) ?,", ", NULL,", command)
        command = none_to_null(command)
        self.db_conn = sqlite3.connect(self.dbPath)
        self.db_crsr = self.db_conn.cursor()
        self.db_crsr.execute(command)

    def get(self, command):
        self.execute(command)
        return self.db_crsr.fetchall()
    
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

    def prefix_table(self, prefix):
        return "_".join([prefix, self.name])

    @property
    def columns(self):
        columns = self.db.get("PRAGMA table_info('{}')".format(self.name))
        return list(map(lambda column: column[1], columns)) 

    @property
    def type(self):
        columns = self.db.get("PRAGMA table_info('{}')".format(self.name))
        return list(map(lambda column: column[2], columns)) 

    @property
    def rows(self):
        rows = self.db.get("SELECT * FROM '{}'".format(self.name))
        return rows


if __name__ == "__main__":
    source_path = sys.argv[1]
    dest_path = sys.argv[2]
    source_db = Db(source_path)
    for table in source_db.tables:
        table = Table(table, source_db)
        val_row = []
        null_row = []
        for val in enumerate(table.type):
            if "VARCHAR" in val[1]:
                val_row.append("a")
                null_row.append("NULL")
            else:
                try:
                    max_val = list(map(lambda row: row[val[0]], table.rows))
                    val_row.append(max(max_val)+1)
                    null_row.append(max(max_val)+2)
                except:
                    val_row.append("1111")
                    null_row.append("1111")

        columns = ",".join(list(map(lambda column: "'" + column + "'", table.columns)))
        source_db.set("INSERT INTO '{}' ({}) \nVALUES \n{}".format(table.name, columns, str(tuple(null_row))))
        source_db.set("INSERT INTO '{}' ({}) \nVALUES \n{}".format(table.name, columns, str(tuple(val_row))))

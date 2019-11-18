import logging
import sqlite3
import os.path
import re
import sys

# Create and configure logger
LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
logging.basicConfig(filename = os.getcwd()+"/ETL_Log.log",
                    level = logging.DEBUG,
                    format = LOG_FORMAT,
                    filemode = 'w')
logger = logging.getLogger()


class Db():
    def __init__ (self, path):
        self.dbPath = path

    def execute(self, command):
        def none_to_null(command):
            return re.sub(r", ?(None) ?,", ", NULL,", command)
        command = none_to_null(command)
        logger.debug("SQL Execute: {}".format(command))
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
    def rows(self):
        rows = self.db.get("SELECT * FROM '{}'".format(self.name))
        return rows

class S_Table(Table):
    def __init__ (self, source_table, source_db, dest_db):
        self.db = dest_db
        self.name = source_table.prefix_table("S")

        self.copy_to_table(self.name, source_table, source_db)

        self.m_table = Table(source_table.prefix_table("M"), dest_db)
        if self.m_table.name in dest_db.tables:
            self.s_diff_m()
        else:
            logger.info("No existing M Tables. Creating a new M Table")
            self.copy_to_table(self.m_table.name, source_table, source_db)

    def copy_to_table(self, table_name, source_table, source_db):
        self.db.set("DROP TABLE IF EXISTS {}".format(table_name))

        command = source_db.get("SELECT sql FROM sqlite_master WHERE tbl_name='{}'".format(source_table.name))[0][0]
        command = command.replace(source_table.name, table_name, 1)
        self.db.set(command)

        columns = ",".join(list(map(lambda column: "'" + column + "'", source_table.columns)))
        values = ",\n".join(str(v) for v in source_table.rows)
        self.db.set("INSERT INTO '{}' ({}) \nVALUES \n{}".format(table_name, columns, values))

    def s_diff_m(self):
        diff = list(set(self.rows) - set(self.m_table.rows))
        logger.debug("S_DIFF_M: {}".format(diff))
        return diff

class X_Table(Table):
    def __init__ (self, source_table, source_db, dest_db):
        self.db = dest_db
        self.name = source_table.prefix_table("X")

if __name__ == "__main__":
    source_path = sys.argv[1]
    dest_path = sys.argv[2]
    source_db = Db(source_path)
    dest_db = Db(dest_path)
    table1 = Table(source_db.tables[0], source_db)
    s_table = S_Table(table1, source_db, dest_db)

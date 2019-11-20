import logging
import sqlite3
import os.path
import re
import sys

LOG_FORMAT = "%(asctime)s ETL_Log[%(lineno)d] %(levelname)s: %(message)s"
logging.basicConfig(filename = os.getcwd()+"/Log_ETL",
                    level = logging.DEBUG,
                    format = LOG_FORMAT,
                    filemode = 'w')
logger = logging.getLogger()

class Db():
    def __init__ (self, path):
        self.path = path

    def execute(self, command):
        def none_to_null(command):
            return re.sub(r", ?(None) ?,", ", NULL,", command)
        command = none_to_null(command)
        logger.debug("SQL Execute: {}".format(command))
        self.db_conn = sqlite3.connect(self.path)
        self.db_crsr = self.db_conn.cursor()
        try:
            self.db_crsr.execute(command)
        except Exception as e:
            logger.error(e)

    def get(self, command):
        self.execute(command)
        result = self.db_crsr.fetchall()
        logger.info("\n".join(str(r) for r in result))
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
        logger.info("New table created with name = {}, db = {}".format(name, db.path))
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

class ETL_Table(Table):
    def __init__ (self, source_table, prefix, db, write=True):
        logger.info("New ETL table created with name = {}, db = {}".format(source_table.name, db.path))
        self.name = "_".join([prefix, source_table.name])
        self.db = db
        if write or self.name not in self.db.tables:
            self.create_from_table(source_table)

    def create_from_table(self, source_table):
        self.db.set("DROP TABLE IF EXISTS {}".format(self.name))
        command = source_table.db.get("SELECT sql FROM sqlite_master WHERE tbl_name='{}'".format(source_table.name))[0][0]
        command = command.replace(source_table.name, self.name, 1)
        self.db.set(command)

    def insert_values(self, values):
        logger.info("{} inserted \n{}".format(self.name, "\n".join(str(v) for v in values)))
        columns = ",".join(list(map(lambda column: "'" + column + "'", self.columns)))
        values = ",\n".join(str(v) for v in values)
        command = "INSERT INTO '{}' ({}) \nVALUES \n{}".format(self.name, columns, values)
        self.db.set("INSERT INTO '{}' ({}) \nVALUES \n{}".format(self.name, columns, values))

    def delete_values(self, values):
        logger.info("{} removed \n{}".format(self.name, "\n".join(str(v) for v in values)))
        for value in values:
            v = [str(self.columns[enum[0]]) + " = '" + str(enum[1]) + "'" for enum in enumerate(list(value))]
            command = """
                    DELETE FROM '{table}'
                    WHERE {v}
                    """.format(table = self.name, v = " AND ".join(v))
            command = re.sub(r"\'(\d+)\'", r"\1", command)
            command =  re.sub(r"= 'None'", "IS NULL", command)
            self.db.set(command)

class Extractor():
    def __init__ (self, source_table, db):
        logger.info("New table created with source = {}, db = {}".format(source_table.name, db.path))
        self.source_table = source_table
        self.db = db

    def load_s_table(self):
        s_table = ETL_Table(self.source_table, "S", self.db, True)
        s_table.insert_values(self.source_table.rows)
        return s_table

    def get_difference(self, a_table, b_table):
        x_table = ETL_Table(self.source_table, "X", self.db, True)
        values = a_table.rows
        try:
            values = list(set(a_table.rows) - set(b_table.rows))
            logger.debug("Extractor: {} minus {}: {}".format(a_table.name, b_table.name, values))
        except Exception as e:
            logger.info("Extractor: No existing {}".format(b_table.name, x_table.name))

            logger.error(e)
        x_table.insert_values(values)
        return x_table

    def clean_rows(self, table):
        e_table = ETL_Table(self.source_table, "E", self.db, True)
        c_table = ETL_Table(self.source_table, "C", self.db, True)

        # NULL and MISSING
        for col in table.columns:
            self.db.set("UPDATE '{table}' SET {col} = 'Unknown Value' WHERE {col} IS NULL".format(table = table.name, col=col))
            self.db.set("UPDATE '{table}' SET {col} = 'Missing Value' WHERE {col} IS NULL".format(table = table.name, col=col))

        # DUPLICATES
        columns = ", ".join(table.columns[1:]) # join tables except for id
        duplicates = self.db.get("SELECT {cols} FROM '{table}' GROUP BY {cols} HAVING COUNT(*) > 1".format(table = table.name, cols = columns))
        for duplicate in duplicates:
            duplicate_condition = [str(table.columns[1:][enum[0]]) + " = '" + str(enum[1]) + "'" for enum in enumerate(duplicate)]
            command = "SELECT * FROM '{table}' WHERE {duplicate_condition}".format(table = table.name, duplicate_condition = " AND ".join(duplicate_condition))
            command = re.sub(r"\'(\d+)\'", r"\1", command)
            command =  re.sub(r"= 'None'", "IS NULL", command)
            rows = self.db.get(command)
            e_table.insert_values(rows)
            table.delete_values(rows)

        # MOVE TO CLEAN TABLE
        c_table.insert_values(table.rows)

        return [e_table, c_table]

if __name__ == "__main__":
    try:
        os.remove(os.getcwd()+"/new.db")
    except:
        pass
    source_path = sys.argv[1]
    dest_path = sys.argv[2]
    source_db = Db(source_path)
    dest_db = Db(dest_path)
    table1 = Table(source_db.tables[0], source_db)
    extractor = Extractor(table1, dest_db)
    s_table = extractor.load_s_table()
    m_table = ETL_Table(table1, "M", dest_db, False)
    x_table = extractor.get_difference(s_table, m_table)
    e_table, c_table = extractor.clean_rows(x_table)
    m_table.insert_values(c_table.rows)

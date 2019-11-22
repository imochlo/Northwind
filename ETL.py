import sqlalchemy as db
from sqlalchemy import Table
import pandas as pd
import logging
import sqlite3
import os.path
import re
import sys

LOG_FORMAT = "%(asctime)s ETL_Log[%(lineno)d] %(levelname)s: %(message)s"
logging.basicConfig(filename = os.getcwd()+"/Log_ETL.log",
                    level = logging.DEBUG,
                    format = LOG_FORMAT,
                    filemode = 'w')
logger = logging.getLogger()

class Db:
    def __init__ (self, file):
        self.engine = db.create_engine('sqlite:///'+file)
        self.conn = self.engine.connect()
        self.metadata = db.MetaData(self.engine)
        self.url = str(self.engine.url)

    def create_table(self, prefix, src, if_exists=True):
        name = "_".join([prefix, src.name])
        table = Table(name, self.metadata)
        if name in self.tables:
            if if_exists:
                logger.warning("Dropping {} table from {}".format(name, self.url))
                table.drop(self.engine)
            else:
                logger.warning("Dropping {} table from {}".format(name, self.url))
                return table
        for column in src.columns:
            logger.debug("""
                        Creating table {table_name}.
                        Adding [column:'{column}',
                        type:{_type},
                        primary_key:{primary_key},
                        nullable:{nullable},
                        foreign_keys:{foreign_keys}]
                        """
                        .format(table_name=name,
                                column=column.name,
                                _type=column.type,
                                primary_key=column.primary_key,
                                nullable=column.nullable,
                                foreign_keys=column.foreign_keys)
                         )
            table.append_column(column.copy())
        table.create(checkfirst=True)
        return table

    @property
    def tables(self):
        return self.engine.table_names()

# class Table:
    # def __init__ (self, table, db):
        # self.table = table
        # self.db = db
        # self.name = table.name
        # self.engine = table.metadata.bind.engine
        # self.df = pd.read_sql_table(self.name, self.engine)

if __name__ == "__main__":
    source_file = sys.argv[1]
    dest_file = sys.argv[2]
    source_db = Db(source_file)
    dest_db = Db(dest_file)

    try:
        path = os.getcwd()+"/"+dest_file
        logger.warning("Removing {}".format(path))
        os.remove(path)
    except:
        pass

    # for table_name in source_db.tables:
    table_name = "Employee"

    table = Table(table_name, source_db.metadata, autoload=True, autoload_with=source_db.engine)
    src_df = pd.read_sql_table(table.name, source_db.url)
    s_table = dest_db.create_table("S", table, True)
    m_table = dest_db.create_table("M", table, False)
    try:
        src_df.to_sql(s_table.name, dest_db.url, if_exists='append', index=False)
    except Exception as e:
        logger.error(e)
    finally:
        s_df = pd.read_sql_table(s_table.name, dest_db.url)
    s_df = pd.read_sql_table(s_table.name, dest_db.url)
    m_df = pd.read_sql_table(m_table.name, dest_db.url)

    # get x_table
    x_df = pd.concat([s_df,m_df]).drop_duplicates(keep=False)
    # clean x_table
    pk = str(s_table.primary_key.columns.values()[0].name)
    subset = list(s_df.columns)
    subset.remove(pk)
    c_df = x_df.drop_duplicates(subset=subset, keep=False)
    c_df = c_df.fillna(-1)
    c_df = c_df.replace("", "Missing")
    print(c_df)
    duplicates = x_df.duplicated(subset=subset, keep=False)
    e_df = x_df[duplicates]
    # e_table
    # c_table
    # table = Table(t, source_db)
    # extractor = Extractor(table, dest_db)
    # s_table = extractor.load_s_table()
    # m_table = ETL_Table(table, "M", dest_db, False)
    # x_table = extractor.get_difference(s_table, m_table)
    # e_table, c_table = extractor.clean_rows(x_table)
    # m_table.insert_values(c_table.rows)

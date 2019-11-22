from sqlalchemy import Table
import logging
import os.path
import pandas as pd
import re
import sqlalchemy as db
import sqlite3
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

    def create_table(self, table, src, if_exists=True):
        logger.debug("Creating table {} from {}".format(table.name, src.name))
        if table.name in self.tables:
            if if_exists:
                logger.warning("Dropping {} table from {}".format(table.name, self.url))
                table.drop(self.engine)
            else:
                logger.warning("Dropping {} table from {}".format(table.name, self.url))
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
                        .format(table_name=table.name,
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

class ETL_Table:
    def __init__ (self, prefix, src, db, if_exists=True):
        name = "_".join([prefix, src.name])
        self.db = db
        self.table = Table(name, db.metadata)
        self.name = self.table.name

        self.db.create_table(self.table, src, if_exists=if_exists)

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
    s_table = ETL_Table("S", table, dest_db, True)
    m_table = ETL_Table("M", table, dest_db, False)
    try:
        src_df.to_sql(s_table.name, dest_db.url, if_exists='append', index=False)
    except Exception as e:
        logger.error(e)
    finally:
        s_df = pd.read_sql_table(s_table.name, dest_db.url)
    s_df = pd.read_sql_table(s_table.name, dest_db.url)
    m_df = pd.read_sql_table(m_table.name, dest_db.url)

    # get x_table
    x_table = ETL_Table("X", table, dest_db, True)
    x_df = pd.concat([s_df,m_df]).drop_duplicates(keep=False)
    x_df.to_sql(x_table.name, dest_db.url, if_exists='append', index=False)
    # clean x_table
    # c_table remove duplicates
    pk = str(s_table.table.primary_key.columns.values()[0].name)
    subset = list(s_df.columns)
    subset.remove(pk)
    c_df = x_df.drop_duplicates(subset=subset, keep=False)
    # c_table replace missing values
    for col in c_df.columns:
        null_value = "Unknown Value" if (c_df[col].dtypes == 'object') else -1
        empty_value = "Missing Value" if (c_df[col].dtypes == 'object') else -2
        c_df[col].fillna(null_value, inplace=True)
        c_df[col].replace("", empty_value, inplace=True)
    c_table = ETL_Table("C", table, dest_db, True)
    c_df.to_sql(c_table.name, dest_db.url, if_exists='append', index=False)
    # e_table get duplicates
    duplicates = x_df.duplicated(subset=subset, keep=False)
    e_df = x_df[duplicates]
    e_table = ETL_Table("E", table, dest_db, True)
    e_df.to_sql(e_table.name, dest_db.url, if_exists='append', index=False)
    # m_table
    c_df.to_sql(m_table.name, dest_db.url, if_exists='append', index=False)
    # e_table
    # c_table
    # table = Table(t, source_db)
    # extractor = Extractor(table, dest_db)
    # s_table = extractor.load_s_table()
    # m_table = ETL_Table(table, "M", dest_db, False)
    # x_table = extractor.get_difference(s_table, m_table)
    # e_table, c_table = extractor.clean_rows(x_table)
    # m_table.insert_values(c_table.rows)

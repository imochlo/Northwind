from sqlalchemy import Table
import logging
import os.path
import pandas as pd
import re
import sqlalchemy as db
import sqlite3
import sys
from datetime import datetime

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

    def get_dtypes(self, table_name):
        result = {}
        try:
            table = Table(table_name, self.metadata, autoload=True, autoload_with=self.engine)
            for column in table.columns:
                result[column.name] = column.type
            logger.info(f"Table {table_name} has data types: {result}")
        except Exception as e:
            logger.error(e)
            logger.error("Data Types not found")
        finally:
            return result

    def get_primary_key(self, table_name):
        result = ""
        try:
            table = Table(table_name, self.metadata, autoload=True, autoload_with=self.engine)
            result = str(table.primary_key.columns.values()[0].name)
            logger.info(f"Table {table_name} has primary key {result}")
        except Exception as e:
            logger.error(e)
            logger.error("Primary key not found")
        finally:
            return result

    @property
    def tables(self):
        return self.engine.table_names()

class Extractor:
    def __init__ (self, src_db, dest_db):
        self.src_db = src_db
        self.dest_db = dest_db
        logger.info(f"Extractor created with source db: {src_db.url} and dest_db: {dest_db.url}")

    def store_df(self, df, table_name, if_exists='replace'):
        df.to_sql(table_name, self.dest_db.url, if_exists = if_exists, index=False, dtype=self.column_types)
        logger.info(f"Extractor {if_exists}(ed) {table_name}\n"
                    f"{df}")
        df.name = table_name
        return df

    def get_diff(self, table, comparator_table):
        result = pd.DataFrame()
        df = pd.read_sql_table(table, self.dest_db.url)
        try:
            comparator_df = pd.read_sql_table(comparator_table, self.dest_db.url)
        except Exception as e:
            result = df.copy()
            logger.error(e)
            logger.warning(f"Extractor copied rows {table}\n"
                           f"{result}")
        else:
            result = pd.concat([df,comparator_df]).drop_duplicates(keep=False)
            logger.info(f"Extractor {table} diff with {comparator_table}\n"
                        f"{result}")
        finally:
            return result

    def get_duplicates(self, df):
        subset = set(df.columns) - set(self.pk)
        result = df.duplicated(subset=list(subset), keep=False)
        logger.info(f"Extractor duplicates {df.name}"
                    f"{result}")
        return result


    def replace_nulls(self, df):
        for col in df.columns:
            null_value = "Unknown Value" if (df[col].dtypes == 'object') else -1
            empty_value = "Missing Value" if (df[col].dtypes == 'object') else -2
            df[col].fillna(null_value, inplace=True)
            df[col].replace("", empty_value, inplace=True)
        logger.info(f"Extractor replaced nulls\n"
                    f"{df}")
        return df


    def extract(self, table_name):
        prefix_table = lambda prefix: "_".join([prefix, table_name])
        try:
            self.column_types = self.src_db.get_dtypes(table_name)
            self.pk = self.src_db.get_primary_key(table_name)
            self.table_name = table_name
            s_df = pd.read_sql_table(table_name, self.src_db.url)
        except Exception as e:
            logger.error(e)
            logger.critical(f"FAILED TO EXTRACT {table_name}")
            return
        else:
            s_df = self.store_df(s_df, prefix_table("S"), 'replace')

        x_df = self.get_diff(s_df.name, prefix_table("M"))
        x_df = self.store_df(x_df, prefix_table("X"), 'replace')

        duplicates = self.get_duplicates(x_df)
        e_df = x_df[duplicates]
        c_df = x_df[~duplicates]
        self.store_df(e_df, prefix_table("E"), 'append')
        self.store_df(c_df, prefix_table("C"), 'replace')

        self.store_df(c_df, prefix_table("M"), 'append')

class Transformer():
    def __init__ (self, db):
        self.db = db

    def has_foreign_key(self, table):
        df = pd.read_sql_table(table, self.db.url)
        columns = list(df.columns)
        print("\n"+table)
        print(columns)
        columns.remove("Id") # remove primary key
        result = list(filter(lambda column: re.search("Id$", column), columns))
        return len(result) > 0

    def transform(self, table):
        while self.has_foreign_key(table):
            table = "C_Employee"
        # while table_column has id
        # get table columns
        # for each table column minus pk
        # if column in (db.tables)+id, then get c_+column-id

if __name__ == "__main__":
    start = datetime.now()

    source_file = sys.argv[1]
    dest_file = sys.argv[2]
    source_db = Db(source_file)
    dest_db = Db(dest_file)

    # try:
        # path = os.getcwd()+"/"+dest_file
        # logger.warning("Removing {}".format(path))
        # os.remove(path)
    # except:
        # pass

    time_stats = []
    # extractor = Extractor(source_db, dest_db)
    # for table_name in source_db.tables:
        # start_time = datetime.now()
        # extractor.extract(table_name)
        # end_time = datetime.now()

        # delta_time = str((end_time-start_time).total_seconds())
        # time_stats.append({table_name:delta_time})
        # logger.info(f"Runtime for table {table_name} : {delta_time} s")

    transformer = Transformer(dest_db)
    clean_tables = filter(lambda table: "C_" in table, dest_db.tables)
    for table_name in clean_tables:
        start_time = datetime.now()
        transformer.transform(table_name)
        end_time = datetime.now()

        delta_time = str((end_time-start_time).total_seconds())
        time_stats.append({table_name:delta_time})
        logger.info(f"Runtime for table {table_name} : {delta_time} s")

    end = datetime.now()
    delta_time = str((end-start).total_seconds())
    logger.info(f"Runtime: {delta_time} s")
    print(f"Runtime for file: {delta_time} s")
    print("Stats:")
    for stat in time_stats:
        print(stat)

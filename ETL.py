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
            result = "Id"
            logger.error(e)
            logger.error(f"Primary key not found, default set {result}")
        finally:
            return result

    def get_table_df(self, table):
        df = pd.DataFrame()
        try:
            df = pd.read_sql_table(table, self.url)
        except Exception as e:
            logger.error(e)
            logger.error(f"{table} from {self.url} does not exist")
        finally:
            df.name = table
            logger.debug(f"Extracted df from {df.name}\n"
                         f"{df}")
            return df

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

    def get_remaining_foreign_keys(self, df):
        print("remaining foreign keys")
        print(df.columns)
        foreign_keys = list(filter(lambda column: re.search(".Id$", column) 
                                   and not re.sub("(.*)Id$", r"\1Key", column) in df.columns,
                                   list(df.columns)))
        result = list(map(lambda key: {"name":re.sub("(.*)Id$", r"C_\1", key),
                                       "key":re.sub("(.*)Id$", r"\1Key", key),
                                       "id":key,
                                       }, foreign_keys))
        logger.debug(f"{df.name} Remaining Foreign Keys: {result}")
        print(result)
        return result

    def join_tables(self, df, f_df, key):
        f_df.insert(0, key["key"], range(1, len(f_df)+1))
        logger.debug(f"Inserted {key} for {f_df.name}"
                     f"{f_df}")
        df = df.merge(f_df.set_index("Id"), left_on=key["id"], right_index=True, how="left")
        df.name = key["name"]
        logger.debug(f"Join Tables {df.name} and {f_df.name} on {key.get(id)}\n"
                     f"{df}")
        return df

    def transform(self, table):
        print(table)
        df = self.db.get_table_df(table)
        foreign_keys = self.get_remaining_foreign_keys(df)
        counter = 0
        while foreign_keys and counter < 5:
            key = foreign_keys.pop(0)
            f_df = self.db.get_table_df(key["name"])
            df = self.join_tables(df, f_df, key)
            foreign_keys = self.get_remaining_foreign_keys(df)
            print(counter)
            counter+=1
        df.insert(0, "Key", range(1, len(df)+1))
        logger.critical(df)
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

    clean_tables = ["C_Employee", "C_Order"]
    transformer = Transformer(dest_db)
    # clean_tables = filter(lambda table: "C_" in table, dest_db.tables)
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

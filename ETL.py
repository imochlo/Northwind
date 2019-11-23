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

    def get_dtypes(self, table_name):
        result = {}
        try:
            table = Table(table_name, self.metadata, autoload=True, autoload_with=self.engine)
            for column in table.columns:
                result[column.name] = column.type
            logger.info(f"Table {table_name} has data types: {result}")
        except Exception as e:
            logger.error(e)
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
        finally:
            return result

    @property
    def tables(self):
        return self.engine.table_names()

class Extractor:
    def __init__ (self, table_name, src_db, dest_db):
        self.src_table = table_name
        self.column_types = src_db.get_dtypes(table_name)
        self.src_db = src_db
        self.dest_db = dest_db
        logger.info(f"Extractor for table {table_name} with source db: {src_db.url} and dest_db: {dest_db.url}")

    def copy_table(self, prefix):
        src_df = pd.read_sql_table(self.src_table, self.src_db.url)
        table_name = "_".join([prefix, self.src_table])
        result = pd.DataFrame()
        try:
            result = pd.read_sql_table(self.src_table, self.src_db.url)
            result.to_sql(table_name, self.dest_db.url, if_exists='replace', index=False, dtype=self.column_types)
        except Exception as e:
            logger.error(e)
        finally:
            logger.info(f"Extractor copied from {self.src_table} to {table_name}\n"
                        f"{result}")
            return result

    def get_new_rows(self, prefix, prefix_comparator):
        name = "_".join([prefix, self.src_table])
        comparator_name = "_".join([prefix_comparator, self.src_table])
        df = pd.read_sql_table(name, self.dest_db.url)
        result = pd.DataFrame()
        try:
            comparator_df = pd.read_sql_table(comparator_name, self.dest_db.url)
            result = pd.concat([df,comparator_df]).drop_duplicates(keep=False)
        except Exception as e:
            logger.error(e)
            result = df
        finally:
            logger.info(f"Extractor got new rows in {name} compared with {comparator_name}\n"
                        f"{result}")
            return result

    def clean_rows(self, df):
        clean = "_".join(["C", self.src_table])
        error = "_".join(["E", self.src_table])
        subset = list(df.columns)
        subset.remove(self.src_db.get_primary_key(self.src_table))
        duplicates = df.duplicated(subset=subset, keep=False)
        error_df = df[duplicates]
        clean_df = df[df[duplicates] == False]
        for col in clean_df.columns:
            null_value = "Unknown Value" if (clean_df[col].dtypes == 'object') else -1
            empty_value = "Missing Value" if (clean_df[col].dtypes == 'object') else -2
            df[col].fillna(null_value, inplace=True)
            df[col].replace("", empty_value, inplace=True)
        clean_df.to_sql(clean, self.dest_db.url, if_exists='replace', index=False, dtype=self.column_types)
        logger.info(f"Extractor got clean rows {clean}"
                    f"{clean_df}")
        error_df.to_sql(error, self.dest_db.url, if_exists='replace', index=False, dtype=self.column_types)
        logger.info(f"Extractor got error rows {error}"
                    f"{error_df}")
        return clean_df


    def extract(self):
        s_df = self.copy_table("S")
        x_table = "_".join(["X", self.src_table])
        x_df = self.get_new_rows("S", "M")
        x_df.to_sql(x_table, self.dest_db.url, if_exists='replace', index=False, dtype=self.column_types)
        c_df = self.clean_rows(x_df)

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
    extractor = Extractor(table_name, source_db, dest_db)
    extractor.extract()

    # table = Table(table_name, source_db.metadata, autoload=True, autoload_with=source_db.engine)
    # src_df = pd.read_sql_table(table.name, source_db.url)
    # s_table = ETL_Table("S", table, dest_db, True)
    # m_table = ETL_Table("M", table, dest_db, False)
    # try:
        # src_df.to_sql(s_table.name, dest_db.url, if_exists='append', index=False)
    # except Exception as e:
        # logger.error(e)
    # finally:
        # s_df = pd.read_sql_table(s_table.name, dest_db.url)
    # s_df = pd.read_sql_table(s_table.name, dest_db.url)
    # m_df = pd.read_sql_table(m_table.name, dest_db.url)

    # # get x_table
    # x_table = ETL_Table("X", table, dest_db, True)
    # x_df = pd.concat([s_df,m_df]).drop_duplicates(keep=False)
    # x_df.to_sql(x_table.name, dest_db.url, if_exists='append', index=False)
    # # clean x_table
    # # c_table remove duplicates
    # pk = str(s_table.table.primary_key.columns.values()[0].name)
    # subset = list(s_df.columns)
    # subset.remove(pk)
    # c_df = x_df.drop_duplicates(subset=subset, keep=False)
    # # c_table replace missing values
    # for col in c_df.columns:
        # null_value = "Unknown Value" if (c_df[col].dtypes == 'object') else -1
        # empty_value = "Missing Value" if (c_df[col].dtypes == 'object') else -2
        # c_df[col].fillna(null_value, inplace=True)
        # c_df[col].replace("", empty_value, inplace=True)
    # c_table = ETL_Table("C", table, dest_db, True)
    # c_df.to_sql(c_table.name, dest_db.url, if_exists='append', index=False)
    # # e_table get plicates
    # duplicates = x_df.duplicated(subset=subset, keep=False)
    # e_df = x_df[duplicates]
    # e_table = ETL_Table("E", table, dest_db, True)
    # e_df.to_sql(e_table.name, dest_db.url, if_exists='append', index=False)
    # # m_table
    # c_df.to_sql(m_table.name, dest_db.url, if_exists='append', index=False)
    # # e_table
    # # c_table
    # # table = Table(t, source_db)
    # # extractor = Extractor(table, dest_db)
    # # s_table = extractor.load_s_table()
    # # m_table = ETL_Table(table, "M", dest_db, False)
    # # x_table = extractor.get_difference(s_table, m_table)
    # # e_table, c_table = extractor.clean_rows(x_table)
    # # m_table.insert_values(c_table.rows)

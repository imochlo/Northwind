
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

    def get_table_df(self, table_name):
        df = pd.DataFrame()
        try:
            df = pd.read_sql_table(table_name, self.url)
        except Exception as e:
            logger.error(e)
            logger.error(f"{table_name} from {self.url} does not exist")
        finally:
            df.name = table_name
            if table_name.find("_"):
                df.root_name = re.sub(r"(.*)_(.*)", r"\2", table_name)
            else:
                df.root_name = table_name
            logger.debug(f"Extracted df from {df.name}\n"
                         f"{df}")
            return df

    @property
    def tables(self):
        return self.engine.table_names()

if __name__ == "__main__":
    start = datetime.now()

    # source_file = sys.argv[1]
    # dest_file = sys.argv[2]
    source_file = "Northwind.sqlite"
    dest_file = "ETL.sqlite"
    source_db = Db(source_file)
    dest_db = Db(dest_file)

    time_stats = []

    # CODE 
    start_date = '1900-01-01'
    end_date = '2100-12-31'
    df = pd.DataFrame({"Date": pd.date_range(start_date, end_date)})
    df["DayName"] = df.Date.dt.weekday_name
    df["Month"] = df.Date.dt.month
    df["Day"] = df.Date.dt.day
    df["Year"] = df.Date.dt.year
    df["Week"] = df.Date.dt.weekofyear
    df["Quarter"] = df.Date.dt.quarter
    df["Year_half"] = (df.Quarter+1) // 2
    df.insert(0, "Key", range(len(df)))
    try:
        df.to_sql("D_Date", "sqlite:///DW.sqlite", if_exists = 'fail', index=False)
    except Exception as e:
        print(e)

    end = datetime.now()
    delta_time = str((end-start).total_seconds())
    logger.info(f"Runtime: {delta_time} s")
    print(f"Runtime for file: {delta_time} s")
    print("Stats:")
    for stat in time_stats:
        print(stat)

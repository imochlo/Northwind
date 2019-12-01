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
log_time = datetime.now().strftime("%d%b%y_%Hh%Mm%Ss")
logging.basicConfig(filename = os.getcwd()+"/Log_ETL"+log_time+".log",
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

    def create_subd_date(self, table_name):
        prefix = re.sub(r"D_(.*)Date", r"\1", table_name)
        df = self.get_table_df("D_Date")
        df = df.add_prefix(prefix)
        df.to_sql(table_name, self.url, if_exists = 'replace', index=False)
        logger.info(f"Create Sub Date {table_name}\n{df.head()}")
        return df

    def get_table_df(self, table_name):
        df = pd.DataFrame()
        try:
            df = pd.read_sql_table(table_name, self.url)
        except Exception as e:
            logger.error(e)
            logger.error(f"{table_name} from {self.url} does not exist")
            if re.match(r"D_.*Date", table_name):
                df = self.create_subd_date(table_name)

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

class Extractor:
    def __init__ (self, src_db, dest_db):
        self.src_db = src_db
        self.dest_db = dest_db
        logger.info(f"Extractor created with source db: {src_db.url} and dest_db: {dest_db.url}")

    def store_df(self, df, table_name, if_exists='replace'):
        try:
            df.to_sql(table_name, self.dest_db.url, if_exists = if_exists, index=False, dtype=self.column_types)
        except Exception as e:
            logger.error(e)
            df.to_sql(table_name, self.dest_db.url, if_exists = if_exists, index=False)
            logger.warning(f"Extractor store_df {if_exists}(ed) {table_name} [dtypes:{df.dtypes}]\n"
                        f"{df}")
        else:
            logger.info(f"Extractor store_df {if_exists}(ed) {table_name} ({self.column_types})\n"
                        f"{df}")
        finally:
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
        subset = list(df.columns)
        subset.remove(self.pk)
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
        c_df = self.replace_nulls(c_df)
        self.store_df(e_df, prefix_table("E"), 'append')
        self.store_df(c_df, prefix_table("C"), 'replace')

        self.store_df(c_df, prefix_table("M"), 'append')


class Transformer():
    def __init__ (self, db):
        self.db = db
        logger.info(f"Transformer created for {self.db.url}")
        if "D_Date" not in db.tables:
            self.create_d_date()

    def create_d_date(self):
        start_date = '1900-01-01'
        end_date = '2100-12-31'
        df = pd.DataFrame({"Date": pd.date_range(start_date, end_date)})
        df.insert(0, "DateId", range(len(df)))
        df["DayNameNum"] = df["Date"].apply(lambda date: str(date.strftime("%w"))).astype(str)
        df["DayNameFull"] = df["Date"].apply(lambda date: str(date.strftime("%A"))).astype(str)
        df["DayNameAbbrev"] = df["Date"].apply(lambda date: str(date.strftime("%a"))).astype(str)
        df["MonthNum"] = df.Date.dt.month
        df["MonthFull"] = df["Date"].apply(lambda date: str(date.strftime("%B"))).astype(str)
        df["MonthAbbrev"] = df["Date"].apply(lambda date: str(date.strftime("%b"))).astype(str)
        df["Day"] = df.Date.dt.day
        df["Year"] = df.Date.dt.year
        df["Week"] = df.Date.dt.weekofyear
        df["Quarter"] = df.Date.dt.quarter
        df["Half"] = (df.Quarter+1) // 2
        df["Date"] = df["Date"].apply(lambda date: str(date.strftime("%Y-%m-%d"))).astype(str)
        try:
            df.to_sql("D_Date", self.db.url, if_exists = 'fail', index=True, index_label="DateKey")
            logger.info(f"D_Date created\n"
                        f"{df}")
        except Exception as e:
            logger.error(e)

    def get_date_keys(self, df):
        result = []
        date_columns = list(filter(lambda col: re.search(r".*Date$", col)
                                   and not col+"Key" in list(df.columns),
                                   list(df.columns)))
        for date_col in date_columns:
            root = re.sub(r"^(.*)Date", r"\1", date_col)
            result.append({"name":"D_"+date_col, 
                           "key": date_col+"Key", 
                           "left_id":date_col,
                           "right_id":date_col,
                           })
        return result

    def get_custom_key(self, df):
        result = []
        columns = list(df.columns)
        if ("ShipVia" in columns and "ShipperKey" not in columns):
            result.append({"name":"C_Shipper",
                                 "key": "ShipperKey",
                                 "left_id": "ShipVia",
                                 "right_id": "Id"
                                 })
        if ("CustomerTypeId" in columns and "CustomerDemographicKey" not in columns):
            result.append({"name":"C_CustomerDemographic",
                                 "key":"CustomerDemographicKey",
                                 "left_id": "CustomerTypeId",
                                 "right_id": "Id"
                                 })
            # foreign_keys = [new_map if x["id"] == "CustomerTypeId" else x for x in foreign_keys]
        return result


    def get_remaining_foreign_keys(self, df):
        res = []
        foreign_keys = list(filter(lambda column: re.search(".Id$", column) 
                                   and not re.sub("(.*)Id$", r"\1Key", column) in df.columns
                                   and re.sub("(.*)Id$", r"C_\1", column) in self.db.tables,
                                   list(df.columns)))
        result = list(map(lambda key: {"name":re.sub("(.*)Id$", r"C_\1", key),
                                       "key":re.sub("(.*)Id$", r"\1Key", key),
                                       "left_id":key,
                                       "right_id":"Id",
                                       }, foreign_keys))
        result.extend(self.get_date_keys(df))
        result.extend(self.get_custom_key(df))
        logger.debug(f"{self.table_name} Remaining Foreign Keys: {result}")
        return result

    def join_tables(self, df, f_df, t_key):
        l_id = t_key["left_id"]
        r_id = t_key["right_id"]
        key = t_key["key"]
        f_name = f_df.name
        if t_key["key"] not in f_df.columns:
            f_df.insert(0, t_key["key"], range(len(f_df)))
            logger.debug(f"Inserted {key} for {f_name}"
                         f"{f_df}")
        conflict_columns = list(filter(lambda col: col in list(f_df.columns) and col != r_id, list(df.columns)))
        if len(conflict_columns) > 0:
            logger.warning(f"Conflict columns {conflict_columns}")
            df_column = {}
            for col in conflict_columns:
                df_column[col] = self.table_name + col
            df.rename(columns=df_column, inplace=True)
            for col in conflict_columns:
                df_column[col] = f_df.root_name + col
            f_df.rename(columns=df_column, inplace=True)

        if l_id == r_id:
            df = df.merge(f_df, on=l_id, how="left")
        else:
            df = df.merge(f_df.set_index(r_id), left_on=l_id, right_index=True, how="left")
        logger.debug(f"Join Tables {self.table_name} and {f_name} on {l_id} = {r_id}\n"
                     f"{df}")
        types = self.db.get_dtypes(t_key["name"])
        return df

    def get_diff(self, table, comparator_table):
        result = pd.DataFrame()
        df = pd.read_sql_table(table, self.db.url)
        try:
            comparator_df = pd.read_sql_table(comparator_table, self.db.url)
        except Exception as e:
            result = df.copy()
            logger.error(e)
            logger.warning(f"Transformer copied rows {table}\n"
                           f"{result}")
        else:
            result = pd.concat([df,comparator_df]).drop_duplicates(keep=False)
            logger.info(f"Transformer {table} diff with {comparator_table}\n"
                        f"{result}")
        finally:
            return result

    def get_duplicates(self, df):
        # To be replaced and used for U_table
        subset = list(df.columns)
        subset.remove("Id")
        result = df.duplicated(subset=list(subset), keep=False)
        logger.info(f"Transformer duplicates {df.name}"
                    f"{result}")
        return result

    def store_df(self, df, table_name, if_exists='replace', index_label=None):
        # columns = {}
        # for col in list(df.columns):
            # print(col)
            # if col.find(self.table_name) != 0:
                # columns[col] = self.table_name+col
                # print(self.table_name+col)
        # df.rename(columns=columns, inplace=True) 
        df.rename(columns={"Id":self.table_name+"Id"}, inplace=True) 
        try:
            if index_label is not None:
                df.to_sql(table_name, self.db.url, if_exists = if_exists, index=True, index_label=index_label)
            else:
                df.to_sql(table_name, self.db.url, if_exists = if_exists, index=False)
        except Exception as e:
            logger.error(e)
            logger.error(f"Transformer unable to store {table_name}")
        else:
            logger.info(f"Transformer store_df {if_exists}(ed) {table_name} [index_label:{index_label}, dtypes:\n{df.dtypes}]\n"
                        f"{df}")
        finally:
            df.name = table_name
            return df

    def get_new_rows(self, df):
        pass

    def changed_rows(self, df):
        pass

    def transform(self, table_name):
        prefix_table = lambda prefix: "_".join([prefix, table_name])
        suffix_table = lambda suffix: "".join([table_name, suffix])
        self.table_name = table_name
        df = self.db.get_table_df(prefix_table("C"))
        foreign_keys = self.get_remaining_foreign_keys(df)
        counter = 0
        while foreign_keys:
            key = foreign_keys.pop(0)
            f_df = self.db.get_table_df(key["name"])
            df = self.join_tables(df, f_df, key)
            foreign_keys = self.get_remaining_foreign_keys(df)
        t_df = self.store_df(df, prefix_table("T"), if_exists = 'replace', index_label=suffix_table("Key"))
        d_df = self.db.get_table_df(prefix_table("D"))
        i_df = self.get_diff(prefix_table("T"), prefix_table("D"))
        self.store_df(i_df, prefix_table("I"), if_exists = 'replace')
        self.store_df(i_df, prefix_table("D"), if_exists = 'replace')
        # Note: update is not applicable and so D is just replaced

class Loader:
    def __init__ (self, src_db, dest_db):
        self.src_db = src_db
        self.dest_db = dest_db
        logger.info(f"Loader created with source db: {src_db.url} and dest_db: {dest_db.url}")

    def load(self, table_name):
        prefix_table = lambda prefix: "_".join([prefix, table_name])
        df = self.src_db.get_table_df(prefix_table("D"))
        df.to_sql(prefix_table("D"), self.dest_db.url, if_exists = 'replace', index=False)

if __name__ == "__main__":
    start = datetime.now()

    # source_file = sys.argv[1]
    # dest_file = sys.argv[2]
    source_file = "Northwind.sqlite"
    etl_file = "ETL.sqlite"
    dw_file = "DW.sqlite"
    source_db = Db(source_file)
    etl_db = Db(etl_file)
    dw_db = Db(dw_file)

    try:
        path = os.getcwd()+"/"+etl_file
        logger.warning("Removing {}".format(path))
        os.remove(path)
    except:
        pass

    try:
        path = os.getcwd()+"/"+dw_file
        logger.warning("Removing {}".format(path))
        os.remove(path)
    except:
        pass

    time_stats = []

    extractor = Extractor(source_db, etl_db)
    # table_names = ["Employee"]
    # for table_name in table_names:
    for table_name in source_db.tables:
        start_time = datetime.now()
        extractor.extract(table_name)
        end_time = datetime.now()

        delta_time = str((end_time-start_time).total_seconds())
        time_stats.append({table_name:delta_time})
        logger.info(f"Extractor Runtime for table {table_name} : {delta_time} s")

    transformer = Transformer(etl_db)
    clean_tables = list(filter(lambda table: "C_"+table in etl_db.tables, source_db.tables))
    # clean_tables = ["Employee", "Order"]
    for table_name in clean_tables:
        start_time = datetime.now()
        transformer.transform(table_name)
        end_time = datetime.now()

        delta_time = str((end_time-start_time).total_seconds())
        time_stats.append({table_name:delta_time})
        logger.info(f"Transformer Runtime for table {table_name} : {delta_time} s")

    loader = Loader(etl_db, dw_db)
    dim_tables = list(filter(lambda table: re.match(r"^D_", table), etl_db.tables))
    # clean_tables = ["Employee", "Order"]
    for table_name in dim_tables:
        start_time = datetime.now()
        loader.load(table_name)
        end_time = datetime.now()

        delta_time = str((end_time-start_time).total_seconds())
        time_stats.append({table_name:delta_time})
        logger.info(f"Transformer Runtime for table {table_name} : {delta_time} s")

    # Fact Tables
    df = pd.read_sql_table("D_OrderDetail", "sqlite:///DW.sqlite")
    df["ExtendedAmt"] = df["Quantity"] * df["OrderDetailUnitPrice"]
    df["TotalAmt"] = (1-df["Discount"]) * df["Quantity"] * df["OrderDetailUnitPrice"]
    df["DiscountAmt"] = df["Discount"] * df["Quantity"] * df["OrderDetailUnitPrice"]

    f_order_line_item = df[[
        "OrderKey",
        "OrderDateKey",
        "RequiredDateKey",
        "CustomerKey",
        "ProductKey",
        "EmployeeKey",
        "OrderDetailUnitPrice",
        "Quantity",
        "Discount",
        "ExtendedAmt",
        "TotalAmt",
        "DiscountAmt",
    ]]

    f_order_line_item = f_order_line_item.groupby([
        "OrderKey",
        "OrderDateKey",
        "RequiredDateKey",
        "CustomerKey",
        "ProductKey",
        "EmployeeKey",
    ]).agg({
        "ExtendedAmt":"sum",
        "TotalAmt":"sum",
        "DiscountAmt":"sum"
    })
    f_order_line_item.to_sql("F_OrderLineItem", dw_db.url, if_exists = 'replace', index=True, index_label="F_OrderLineItemKey")

    f_order_transaction = df[[
        "OrderKey",
        "OrderDateKey",
        "RequiredDateKey",
        "CustomerKey",
        "EmployeeKey",
        "Quantity",
        "ExtendedAmt",
        "TotalAmt",
        "DiscountAmt",
    ]]

    f_order_transaction = f_order_transaction.groupby([
        "OrderKey",
        "OrderDateKey",
        "RequiredDateKey",
        "CustomerKey",
        "EmployeeKey",
    ]).agg({
        "Quantity":"sum",
        "ExtendedAmt":"sum",
        "TotalAmt":"sum",
        "DiscountAmt":"sum"
    })
    f_order_transaction.to_sql("F_OrderTransaction", dw_db.url, if_exists = 'replace', index=True, index_label="F_OrderTransaction")

    f_shipment_transaction = df[[
        "OrderKey",
        "OrderDateKey",
        "RequiredDateKey",
        "ShippedDateKey",
        "CustomerKey",
        "EmployeeKey",
        "ShipperKey",
        "Quantity",
        "ExtendedAmt",
        "TotalAmt",
        "DiscountAmt",
        "Freight",
    ]]

    f_shipment_transaction = f_shipment_transaction.groupby([
        "OrderKey",
        "OrderDateKey",
        "RequiredDateKey",
        "ShippedDateKey",
        "CustomerKey",
        "EmployeeKey",
        "ShipperKey"
    ]).agg({
        "Quantity":"sum",
        "ExtendedAmt":"sum",
        "TotalAmt":"sum",
        "DiscountAmt":"sum",
        "Freight":"sum"
    })
    f_shipment_transaction.to_sql("F_ShipmentTransaction", dw_db.url, if_exists = 'replace', index=True, index_label="F_ShipmentTransaction")

    end = datetime.now()
    delta_time = str((end-start).total_seconds())
    logger.info(f"Runtime: {delta_time} s")
    print(f"Runtime for file: {delta_time} s")
    print("Stats:")
    for stat in time_stats:
        print(stat)

import sqlalchemy as db
from sqlalchemy import Table
import re
import os.path
import pandas as pd

# file = os.getcwd() + "/Northwind.sqlite"
file = "Northwind.db"
src_engine = db.create_engine('sqlite:///'+file)
src_conn = src_engine.connect()
src_metadata = db.MetaData(src_engine)

dest_engine = db.create_engine('sqlite:///'+"new.db")
dest_conn = dest_engine.connect()
dest_metadata = db.MetaData(dest_engine)

# GET LIST OF TABLE NAMES
# print(src_engine.table_names())
print(src_engine.table_names())
src_table = Table("Employee", src_metadata, autoload=True, autoload_with=src_engine)
print(src_table.columns)

dest_table = Table("New_Employee", dest_metadata)
print(dest_engine.table_names())
for column in src_table.columns:
    dest_table.append_column(column.copy())
print(dest_table.info)
dest_table.create(checkfirst=True)
print(dest_table.columns)
print(dest_table.primary_key.columns.values()[0])

# print(employee.primary_key.columns.values()[0])
# for col in employee.columns:
    # print(col.name)
    # print(col.type)
# df = pd.read_sql_table("Employee", src_conn)
# duplicates = df.duplicated(subset=df.columns[1:], keep=False)
# print(df[duplicates])

# GET TABLE INFORMATION
# print(df.info(verbose=True))

# df.to_sql('test', con=src_engine, if_exists='replace')
# df.to_sql('test', con=src_engine, if_exists='replace', index=False)


import pandas as pd
import re

pd.set_option('display.max_columns', None)
df = pd.read_sql_table("F_ShipmentTransaction", "sqlite:///DW.sqlite")

quantity = df.groupby("ShipperKey").agg({"Quantity":"sum"})
totamt = df.groupby("ShipperKey").agg({"TotalAmt":"sum"})
print(quantity)
print(totamt)

df = pd.read_sql_table("F_OrderLineItem", "sqlite:///DW.sqlite")
date = pd.read_sql_table("D_OrderDate", "sqlite:///DW.sqlite")
quantity = df.groupby("OrderDateKey", as_index=False).agg({"Quantity":"sum"})
totamt = df.groupby("OrderDateKey", as_index=False).agg({"TotalAmt":"sum"})
quantity = quantity.merge(date)
print(quantity)
print(totamt)

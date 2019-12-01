import pandas as pd
import re
from sqlalchemy.types import NVARCHAR

pd.set_option('display.max_columns', None)
s_order_detail = pd.read_sql_table("OrderDetail", "sqlite:///Northwind.sqlite")
date = pd.read_sql_table("D_Date", "sqlite:///DW.sqlite")
employee = pd.read_sql_table("D_Employee", "sqlite:///DW.sqlite")
order_detail = pd.read_sql_table("D_OrderDetail", "sqlite:///DW.sqlite")
# print(order.columns)
# print(order.head(5))
# print(order["ShipVia"].unique())
# print(order["ShipCountry"].unique())
# print(order.groupby("ShipCountry").size())
# print(employee.head(5))
# print(employee.columns)
# print(date.columns)
# print(date.tail(5))
def join_tables(df, f_df, prefix):
    # print(df.columns.intersection(f_df.columns))
    df = df.add_prefix(prefix)
    return df.merge(f_df,  how="inner")

dates = list(filter(lambda col: "Date" in col, order_detail.columns))
df = order_detail
for d in dates:
    prefix = re.sub(r"^(.*)Date", r"\1", d)
    df = join_tables(date, df, prefix)
# print(df.columns)
# print(df.head(5))
hmm = order_detail.select_dtypes(include=['int', 'float'])
# print(hmm.columns)
# print(order_detail.columns)
df["ExtendedAmt"] = df["Quantity"] * df["OrderDetailUnitPrice"]
df["TotalAmt"] = (1-df["Discount"]) * df["Quantity"] * df["OrderDetailUnitPrice"]
df["DiscountAmt"] = df["Discount"] * df["Quantity"] * df["OrderDetailUnitPrice"]
# Order Line Item Fact
# print(df[["OrderKey", "OrderDateKey", "RequiredDateKey", "CustomerKey", "EmployeeKey", "OrderDetailUnitPrice", "Quantity", "ExtendedAmt", "Discount", "DiscountAmt", "TotalAmt"]].tail())
# Order Transaction
# f_order_transaction = df.groupby("OrderId")#[["ExtendedAmt", "DiscountAmt", "TotalAmt",]].sum()
# print(f_order_transaction["ProductKey"].count().tail())
# print(len(f_order_transaction))
# print(f_order_transaction[["OrderKey", "OrderDateKey", "RequiredDateKey", "CustomerKey", "EmployeeKey", "OrderDetailUnitPrice", "Quantity", "ExtendedAmt", "Discount", "DiscountAmt", "TotalAmt"]].tail())
# Order Transaction
# print(df.columns)
# Shipment Transaction
f_shipment_transaction = df.groupby(
    ["OrderId",
     "OrderDate",
     "RequiredDate",
     "ShippedDate",
     "CustomerKey",
     "EmployeeKey",
     "ShipperKey"
     ]).agg(
         {"Quantity":"count",
          "ExtendedAmt":"sum",
          "TotalAmt":"sum",
          "DiscountAmt":"sum"
          }).reset_index()

# f_shipment_transaction = f_shipment_transaction["ProductKey"].count()
print(f_shipment_transaction.tail())

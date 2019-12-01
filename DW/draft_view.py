import pandas as pd
import re

pd.set_option('display.max_columns', None)
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
}).reset_index()
print(len(f_order_line_item))

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
}).reset_index()
print(len(f_order_transaction))

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
}).reset_index()
print(len(f_shipment_transaction))

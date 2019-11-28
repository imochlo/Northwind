import pandas as pd

df = pd.read_sql_table("D_OrderDetail", "sqlite:///DW.sqlite")
print(df.columns)

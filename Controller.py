import pandas as pd
import datetime as dt
from dateutil import parser
db = "sqlite:///DW/DW.sqlite"

if __name__ == "__main__":
    df = pd.read_sql_table("D_Order", db)
    date = df["OrderDate"].head()[0]
    print(date)
    print(parser.parse(date))

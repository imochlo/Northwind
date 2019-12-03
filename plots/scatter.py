import numpy as np
import pandas as pd
import plotly.offline as pyo
import plotly.graph_objs as go

def numOfOrderedProducts_vs_Month(N=50):
    df = pd.read_sql_table("F_OrderLineItem", "sqlite:///../DW/DW.sqlite")
    date = pd.read_sql_table("D_OrderDate", "sqlite:///../DW/DW.sqlite")
    quantity = df.groupby("OrderDateKey", as_index=False).agg({"Quantity":"sum"})
    quantity = quantity.merge(date)
    quantity = quantity.sort_values(by=["OrderMonthNum"])

    data = go.Scatter(
        x = quantity["OrderMonthAbbrev"],
        y = quantity["Quantity"],
        mode='markers',
    )

    layout = go.Layout(
        title = 'Number of Ordered Products vs. Month', # Graph title
        xaxis = dict(title = 'Month'), # x-axis label
        yaxis = dict(title = 'Number of Ordered Products'), # y-axis label
        hovermode ='closest' # handles multiple points landing on the same vertical
    )
            
        # return dict(data=[trace1])
    fig = go.Figure(data=[data], layout=layout)
    pyo.plot(fig, filename='numOfOrderedProducts_vs_Month')

def totAmt_vs_time(N=50):
    df = pd.read_sql_table("F_OrderLineItem", "sqlite:///../DW/DW.sqlite")
    date = pd.read_sql_table("D_OrderDate", "sqlite:///../DW/DW.sqlite")
    quantity = df.merge(date)
    quantity = quantity.groupby(["OrderYear", "OrderMonthNum"], as_index=False).agg({"TotalAmt":"sum", "Quantity":"sum"})
    quantity["x"] = quantity[["OrderYear", "OrderMonthNum"]].astype(str).apply('-'.join, axis=1)
    print(quantity)

    data = go.Scatter(
        x = quantity["x"],
        y = quantity["TotalAmt"],
        mode='lines+markers',
    )

    layout = go.Layout(
        title = 'Number of Ordered Products vs. Time', # Graph title
        xaxis = dict(title = 'Time'), # x-axis label
        yaxis = dict(title = 'Number of Ordered Products'), # y-axis label
        hovermode ='closest' # handles multiple points landing on the same vertical
    )
            
        # return dict(data=[trace1])
    fig = go.Figure(data=[data], layout=layout)
    pyo.plot(fig, filename='numOfOrderedProducts_vs_Time')

def quantitySold_vs_time(N=50):
    df = pd.read_sql_table("F_OrderLineItem", "sqlite:///../DW/DW.sqlite")
    date = pd.read_sql_table("D_OrderDate", "sqlite:///../DW/DW.sqlite")
    quantity = df.merge(date)
    quantity = quantity.groupby(["OrderYear", "OrderMonthNum"], as_index=False).agg({"TotalAmt":"sum", "Quantity":"sum"})
    quantity["x"] = quantity[["OrderYear", "OrderMonthNum"]].astype(str).apply('-'.join, axis=1)
    print(quantity)

    data = go.Scatter(
        x = quantity["x"],
        y = quantity["Quantity"],
        mode='lines+markers',
    )

    layout = go.Layout(
        title = 'Number of Ordered Products vs. Time', # Graph title
        xaxis = dict(title = 'Time'), # x-axis label
        yaxis = dict(title = 'Number of Ordered Products'), # y-axis label
        hovermode ='closest' # handles multiple points landing on the same vertical
    )
            
        # return dict(data=[trace1])
    fig = go.Figure(data=[data], layout=layout)
    pyo.plot(fig, filename='numOfOrderedProducts_vs_Time')

def quantitySold_vs_time2(N=50):
    df = pd.read_sql_table("F_OrderLineItem", "sqlite:///../DW/DW.sqlite")
    date = pd.read_sql_table("D_OrderDate", "sqlite:///../DW/DW.sqlite")
    quantity = df.merge(date)
    order = pd.read_sql_table("D_OrderDetail", "sqlite:///../DW/DW.sqlite")
    df = df.merge(date)
    df = df.merge(order)
    df = df.groupby(["OrderYear", "OrderMonthNum", "ShipRegion"], as_index=False).agg({"TotalAmt":"sum", "Quantity":"sum"})
    df["CompoundedDate"] = df[["OrderYear", "OrderMonthNum"]].astype(str).apply('-'.join, axis=1)

    data = []
    for r in df["ShipRegion"].unique():
        result = df[df["ShipRegion"] == str(r)]
        trace = go.Scatter(
            x = result["CompoundedDate"],
            y = result["Quantity"],
            name = r,
            mode='markers',
            marker=dict(size=0.002*result["TotalAmt"])
        )
        print(result["TotalAmt"])
        data.append(trace)


    layout = go.Layout(
        title = 'Number of Ordered Products vs. Time', # Graph title
        xaxis = dict(title = 'Time'), # x-axis label
        yaxis = dict(title = 'Number of Ordered Products'), # y-axis label
        hovermode ='closest' # handles multiple points landing on the same vertical
    )
    fig = go.Figure(data=data, layout=layout)
    pyo.plot(fig, filename='numOfOrderedProducts_vs_Time')

def complete_deliveries():
    df = pd.read_sql_table("D_OrderDetail", "sqlite:///../DW/DW.sqlite")
    result = df[df["ShippedDate"] != "Unknown Value"]
    print(len(result))
    print((len(result)/len(df))*100)

def late_deliveries():
    df = pd.read_sql_table("D_OrderDetail", "sqlite:///../DW/DW.sqlite")
    filtered = df[df["ShippedDate"] != "Unknown Value"]
    result = filtered[df["ShippedDate"] > df["RequiredDate"]]
    print(len(result))
    print((len(result)/len(filtered))*100)
    result = filtered[df["ShippedDate"] <= df["RequiredDate"]]
    print(len(result))
    print((len(result)/len(filtered))*100)
    result = df[df["ShippedDate"] == "Unknown Value"]


if __name__ == "__main__":
    # complete_deliveries()
    # late_deliveries()
    # numOfOrderedProducts_vs_Month()
    # totAmt_vs_time()
    # quantitySold_vs_time()
    quantitySold_vs_time2()

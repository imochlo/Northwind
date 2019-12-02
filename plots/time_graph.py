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

def numOfOrderedProducts_vs_Time(N=50):
    df = pd.read_sql_table("F_OrderLineItem", "sqlite:///../DW/DW.sqlite")
    date = pd.read_sql_table("D_OrderDate", "sqlite:///../DW/DW.sqlite")
    quantity = df.groupby("OrderDateKey", as_index=False).agg({"Quantity":"sum"})
    quantity = quantity.merge(date)

    data = go.Scatter(
        x = quantity["OrderDate"],
        y = quantity["Quantity"],
        mode='lines',
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

if __name__ == "__main__":
    numOfOrderedProducts_vs_Time()

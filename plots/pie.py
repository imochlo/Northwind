import numpy as np
import pandas as pd
import plotly.offline as pyo
import plotly.graph_objs as go

def numOfShippedProducts_vs_Shipper(N=50):
    df = pd.read_sql_table("F_ShipmentTransaction", "sqlite:///../DW/DW.sqlite")
    shipper = pd.read_sql_table("D_Shipper", "sqlite:///../DW/DW.sqlite")

    quantity = df.groupby("ShipperKey", as_index=False).agg({"Quantity":"sum"})
    quantity = quantity.merge(shipper)
    totamt = df.groupby("ShipperKey", as_index=False).agg({"TotalAmt":"sum"})

    data = go.Pie(
        labels = quantity["CompanyName"],
        values = quantity["Quantity"],
        textfont = {"size":25,
                    "color":"white"},
        textinfo = "value+percent"
    )

    layout = go.Layout(
        title = 'Number of Shipped Products Per Shipping Company', # Graph title
        font = {"size":20}
    )
            
        # return dict(data=[trace1])
    fig = go.Figure(data=[data], layout=layout)
    pyo.plot(fig, filename='numOfShippedProducts_vs_Shipper.html')

def totAmt_vs_Shipper(N=50):
    df = pd.read_sql_table("F_ShipmentTransaction", "sqlite:///../DW/DW.sqlite")
    shipper = pd.read_sql_table("D_Shipper", "sqlite:///../DW/DW.sqlite")

    totamt = df.groupby("ShipperKey", as_index=False).agg({"TotalAmt":"sum"})
    totamt = totamt.merge(shipper)

    data = go.Bar(
        x = totamt["CompanyName"],
        y = totamt["TotalAmt"],
    )

    layout = go.Layout(
        title = 'Total Amount vs. Shipper', # Graph title
        xaxis = {"title": "Shipper"},
        yaxis = {"title": "Total Amount"},
    )
            
        # return dict(data=[trace1])
    fig = go.Figure(data=[data], layout=layout)
    pyo.plot(fig, filename='totAmt_vs_Shipper')

def quantity_vs_Category(N=50):
    df = pd.read_sql_table("F_OrderLineItem", "sqlite:///../DW/DW.sqlite")
    product = pd.read_sql_table("D_Product", "sqlite:///../DW/DW.sqlite")
    category = pd.read_sql_table("D_Category", "sqlite:///../DW/DW.sqlite")

    result = df.groupby("ProductKey", as_index=False).agg({"Quantity":"sum"})
    result = result.merge(product)
    result = result.merge(category)

    data = go.Bar(
        x = result["CategoryName"],
        y = result["Quantity"],
    )

    layout = go.Layout(
        title = 'Quantity vs. Category', # Graph title
        xaxis = {"title": "Category"},
        yaxis = {"title": "Quantity"},
    )
            
        # return dict(data=[trace1])
    fig = go.Figure(data=[data], layout=layout)
    pyo.plot(fig, filename='quantity_vs_Category.html')

def totalamt_vs_category(N=50):
    df = pd.read_sql_table("F_OrderLineItem", "sqlite:///../DW/DW.sqlite")
    product = pd.read_sql_table("D_Product", "sqlite:///../DW/DW.sqlite")
    category = pd.read_sql_table("D_Category", "sqlite:///../DW/DW.sqlite")

    result = df.groupby("ProductKey", as_index=False).agg({"TotalAmt":"sum"})
    result = result.merge(product)
    result = result.merge(category)

    data = go.Bar(
        x = result["CategoryName"],
        y = result["TotalAmt"],
    )

    layout = go.Layout(
        title = 'Quantity vs. Category', # Graph title
        xaxis = {"title": "Category"},
        yaxis = {"title": "Total Amount"},
    )
            
        # return dict(data=[trace1])
    fig = go.Figure(data=[data], layout=layout)
    pyo.plot(fig, filename='quantity_vs_Category.html')

def quantity_vs_supplier(N=50):
    df = pd.read_sql_table("F_Purchase_Order", "sqlite:///../DW/DW.sqlite")
    supplier = pd.read_sql_table("D_Supplier", "sqlite:///../DW/DW.sqlite")

    result = df.groupby("SupplierKey", as_index=False).agg({"Quantity":"sum", "TotalAmt":"sum"})
    result = result.merge(supplier)

    data = go.Pie(
        labels = result["Region"],
        values = result["Quantity"],
        textfont = {"size":25,
                    "color":"white"},
        textinfo = "value+percent"
    )

    layout = go.Layout(
        title = 'Quantity vs. Category', # Graph title
        xaxis = {"title": "Company Name"},
        yaxis = {"title": "Quantity"},
    )
            
        # return dict(data=[trace1])
    fig = go.Figure(data=[data], layout=layout)
    pyo.plot(fig, filename='quantity_vs_Supplier.html')

if __name__ == "__main__":
    # numOfShippedProducts_vs_Shipper()
    # totAmt_vs_Shipper()
    # totalamt_vs_category()
    # print(go.Figure())
    quantity_vs_supplier()

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

    result = df.merge(product)
    result = result.merge(category)
    result = result.groupby("CategoryName", as_index=False).agg({"Quantity":"sum"})
    print(result)

    # data = go.Bar(
        # x = result["CategoryName"],
        # y = result["Quantity"],
        # text = result["Quantity"],
        # textposition = 'auto'
    # )

    data = go.Pie(
        labels = result["CategoryName"],
        values = result["Quantity"],
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

def supplier_inventory(N=50):
    df = pd.read_sql_table("D_Product", "sqlite:///../DW/DW.sqlite")
    # shipper = pd.read_sql_table("D_Shipper", "sqlite:///../DW/DW.sqlite")

    result = df.groupby(["CompanyName", "CategoryName"], as_index=False).agg({"UnitsInStock":"sum"})
    data = []
    for company in result["CompanyName"].unique():
        df = result[result["CompanyName"] == company]
        data.append(go.Bar(name=company,
                          x = df["CategoryName"],
                          y = df["UnitsInStock"]
                          ))

    # totamt = totamt.merge(shipper)

    # data = go.Bar(
        # x = totamt["CompanyName"],
        # y = totamt["TotalAmt"],
    # )

    layout = go.Layout(
        title = 'Total Amount vs. Shipper', # Graph title
        xaxis = {"title": "Category Name"},
        yaxis = {"title": "Units In Stock", 'categoryorder':'category ascending'},
        barmode = 'stack'
    )
            
        # # return dict(data=[trace1])
    fig = go.Figure(data=data, layout=layout)
    pyo.plot(fig, filename='totAmt_vs_Shipper')

def reorder_level(N=50):
    df = pd.read_sql_table("D_Product", "sqlite:///../DW/DW.sqlite")
    # shipper = pd.read_sql_table("D_Shipper", "sqlite:///../DW/DW.sqlite")

    result = df.groupby(["Region", "CategoryName"], as_index=False).agg({"UnitsInStock":"sum", "UnitsOnOrder":"sum"})
    data = []
    for company in result["Region"].unique():
        df = result[result["Region"] == company]
        data.append(go.Bar(name=company,
                          x = df["CategoryName"],
                          y = df["UnitsInStock"]
                          ))

    data2 = []
    for company in result["Region"].unique():
        df = result[result["Region"] == company]
        data2.append(go.Bar(name=company,
                          x = df["CategoryName"],
                          y = df["UnitsOnOrder"]
                          ))

    # totamt = totamt.merge(shipper)

    # data = go.Bar(
        # x = totamt["CompanyName"],
        # y = totamt["TotalAmt"],
    # )

    layout = go.Layout(
        title = 'Total Amount vs. Shipper', # Graph title
        xaxis = {"title": "Category Name"},
        yaxis = {"title": "Units In Stock", 'categoryorder':'category ascending'},
        barmode = 'stack'
    )

    layout2 = go.Layout(
        title = 'Total Amount vs. Shipper', # Graph title
        xaxis = {"title": "Category Name"},
        yaxis = {"title": "Units In Stock", 'categoryorder':'category ascending'},
        barmode = 'group'
    )
            
        # # return dict(data=[trace1])
    fig = go.Figure(data=data, layout=layout)
    fig2 = go.Figure(data=[data, data2], layout=layout2)
    pyo.plot(fig2, filename='totAmt_vs_Shipper')

def backorders(N=50):
    df = pd.read_sql_table("D_Product", "sqlite:///../DW/DW.sqlite")

    df["UnitsBackordered"] = df["UnitsOnOrder"] - df["UnitsInStock"]
    df = df[df["UnitsBackordered"] > 0]
    df = df[["ProductName", "UnitsBackordered"]].sort_values(by='UnitsBackordered', ascending=False)
    # result = df.groupby(["Region", "CategoryName"], as_index=False).agg({"UnitsInStock":"sum", "UnitsOnOrder":"sum"})
    data = go.Bar(x = df["ProductName"],
                  y = df["UnitsBackordered"]
                  )

    layout = go.Layout(
        title = 'Number of Backordered Items', # Graph title
        xaxis = {"title": "Product Name"},
        yaxis = {"title": "Number of Backorders"},
    )
    fig = go.Figure(data=[data], layout=layout)
    pyo.plot(fig, filename='backorders.html')


def reorder(N=50):
    df = pd.read_sql_table("D_Product", "sqlite:///../DW/DW.sqlite")
    df["UnitSlack"] = df["ReorderLevel"] - df["UnitsInStock"]
    df = df[df["UnitsInStock"] < df["ReorderLevel"]]

    data1 = go.Bar(x = df["ProductName"],
                  y = df["UnitsInStock"],
                  name = "Units In Stock",
                        marker = dict(color='rgb(234, 117, 0)',
                                  opacity = 0.75)
                  )
    data2 = go.Bar(x = df["ProductName"],
                      y = df["UnitSlack"],
                      name = "Reorder Level",
                    marker = dict(color='rgb(234, 0, 0)',
                                      opacity = 0.75)
                      # mode = 'markers',
                      # marker = dict(      # change the marker style
                          # size = 30,
                          # color = 'rgb(255, 0, 0)',
                          # symbol = 'line-ew-open',
                          # line = dict(width = 5)
                     )

    layout = go.Layout(
        xaxis = {"title": "Product Name"},
        yaxis = {"title": "Number of Backorders"},
        barmode = 'stack'
    )
    fig = go.Figure(data=[data1, data2], layout=layout)
    pyo.plot(fig, filename='reorder_level')

if __name__ == "__main__":
    # numOfShippedProducts_vs_Shipper()
    # totAmt_vs_Shipper()
    # totalamt_vs_category()
    # print(go.Figure())
    # quantity_vs_Category()
    # quantity_vs_supplier()
    # supplier_inventory()
    # backorders()
    reorder()

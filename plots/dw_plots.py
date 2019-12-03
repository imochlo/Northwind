import numpy as np
import pandas as pd
import plotly.offline as pyo
import plotly.graph_objs as go
import pycountry


order_detail_dim = pd.read_sql_table("D_OrderDetail", "sqlite:///../DW/DW.sqlite")
shipper_dim = pd.read_sql_table("D_Shipper", "sqlite:///../DW/DW.sqlite")
order_line_item = pd.read_sql_table("F_OrderLineItem", "sqlite:///../DW/DW.sqlite")
order_date = pd.read_sql_table("D_OrderDate", "sqlite:///../DW/DW.sqlite")
shipment_transaction = pd.read_sql_table("F_ShipmentTransaction", "sqlite:///../DW/DW.sqlite")
product_dim = pd.read_sql_table("D_Product", "sqlite:///../DW/DW.sqlite")

def get_completed_deliveries_dict():
    result = order_detail_dim[order_detail_dim["ShippedDate"] != "Unknown Value"]
    value = len(result)
    percentage = (len(result)/len(order_detail_dim))*100
    return {"value":value,
            "percentage":percentage}

def get_late_deliveries_dict():
    result = order_detail_dim[(order_detail_dim["ShippedDate"] != "Unknown Value") &
                              (order_detail_dim["ShippedDate"] > order_detail_dim["RequiredDate"])]
    value = len(result)
    percentage = (len(result)/len(order_detail_dim))*100
    return {"value":value,
            "percentage":percentage}

def get_on_time_deliveries_dict():
    result = order_detail_dim[(order_detail_dim["ShippedDate"] != "Unknown Value") &
                              (order_detail_dim["ShippedDate"] <= order_detail_dim["RequiredDate"])]
    value = len(result)
    percentage = (len(result)/len(order_detail_dim))*100
    return {"value":value,
            "percentage":percentage}

def get_undelivered_dict():
    result = order_detail_dim[order_detail_dim["ShippedDate"] == "Unknown Value"]
    value = len(result)
    percentage = (len(result)/len(order_detail_dim))*100
    return {"value":value,
            "percentage":percentage}

def get_amt_sold_over_time_bubble():
    df = order_line_item.merge(order_detail_dim)
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
            marker=dict(size=0.002*result["TotalAmt"]),
            text = df["TotalAmt"].apply(lambda amt: "Total Sales: " + str(amt))
        )
        data.append(trace)

    layout = go.Layout(
        title = 'Number of Items sold per Region over Time', # Graph title
        yaxis = dict(title = 'Number of Items sold'), # y-axis label
        hovermode ='closest' # handles multiple points landing on the same vertical
    )
    return go.Figure(data=data, layout=layout)

def get_total_sales_over_time_series():
    df = order_line_item.merge(order_date)
    df = df.groupby(["OrderYear", "OrderMonthNum"], as_index=False).agg({"TotalAmt":"sum", "Quantity":"sum"})
    df["CompoundedDate"] = df[["OrderYear", "OrderMonthNum"]].astype(str).apply('-'.join, axis=1)

    data = go.Scatter(
        x = df["CompoundedDate"],
        y = df["TotalAmt"],
        mode='lines+markers',
        text = df["TotalAmt"].apply(lambda amt: "Total Sales: " + str(amt))
    )

    layout = go.Layout(
        title = 'Total Sales over Time', # Graph title
        yaxis = dict(title = 'Total Sales'), # y-axis label
        hovermode ='closest' # handles multiple points landing on the same vertical
    )
    return go.Figure(data=[data], layout=layout)

def get_total_sales_per_country_heatmap():
    df = order_line_item.merge(order_detail_dim)
    countries = {}
    def get_code(c):
        for country in pycountry.countries:
            if c in country.name or c == country.alpha_3:
                return country.alpha_3

    df = df.groupby(["ShipCountry"], as_index=False).agg({"TotalAmt":"sum", "Quantity":"sum"})
    df.replace({"ShipCountry":"UK"}, {"ShipCountry":"United Kingdom"}, inplace=True)
    df["Code"] = df["ShipCountry"].astype(str).apply(lambda country: get_code(country))

    data = go.Choropleth(
        locations = df['Code'],
        z = df['TotalAmt'],
        text = df[["ShipCountry","TotalAmt"]].apply(lambda vals: "Country: " + str(vals["ShipCountry"]) + 
                                                    "<br>Total Sales: " + str(round(vals["TotalAmt"], 2)), axis=1),
        colorscale = 'Greens',
        autocolorscale=False,
        reversescale=True,
        marker = dict(line=dict(width=0.5))
    )

    layout = go.Layout(
        title='2014 Global GDP',
        geo=dict( showframe=False),
    )
    return go.Figure(data=[data], layout=layout)

def get_num_of_shipped_items_per_shipper_pie():
    df = shipment_transaction.merge(shipper_dim)
    df = df.groupby("CompanyName", as_index=False).agg({"Quantity":"sum"})

    data = go.Pie(
        labels = df["CompanyName"],
        values = df["Quantity"],
        textfont = {"size":25,
                    "color":"white"},
        textinfo = "value+percent"
    )

    layout = go.Layout(
        title = 'Number of Shipped Products Per Shipping Company', # Graph title
        font = {"size":20}
    )
    return go.Figure(data=[data], layout=layout)

def get_backorder_bar():
    df = product_dim.copy()
    df["UnitsBackordered"] = df["UnitsOnOrder"] - df["UnitsInStock"]
    df = df[df["UnitsBackordered"] > 0]
    df = df[["ProductName", "UnitsBackordered"]].sort_values(by='UnitsBackordered', ascending=False)
    data = go.Bar(x = df["ProductName"],
                  y = df["UnitsBackordered"],
                  )

    layout = go.Layout(
        title = 'Number of Backordered Items', # Graph title
        xaxis = {"title": "Product Name"},
        yaxis = {"title": "Number of Backorders"},
    )
    return go.Figure(data=[data], layout=layout)

def get_reorder_bar():
    df = product_dim.copy()
    df["UnitSlack"] = df["ReorderLevel"] - df["UnitsInStock"]
    df["Delta"] = df[["ReorderLevel","UnitsInStock"]].apply(lambda vals: vals["UnitsInStock"]-vals["ReorderLevel"],axis=1)
    df = df[df["UnitsInStock"] < df["ReorderLevel"]].sort_values(by="Delta")

    in_stock = go.Bar(x = df["ProductName"],
                   y = df["UnitsInStock"],
                   name = "Units In Stock",
                   marker = dict(color='rgb(234, 117, 0)',
                                 opacity = 0.75))
    reorder_level = go.Bar(x = df["ProductName"],
                           y = df["UnitSlack"],
                           name = "Reorder Level",
                           marker = dict(color='rgb(234, 0, 0)',
                                         opacity = 0.75), 
                           text = df["Delta"],
                           textposition='auto',
                           textfont=dict(color="white",size=30))
    layout = go.Layout(
        xaxis = {"title": "Product Name"},
        yaxis = {"title": "Number of Backorders"},
        barmode = 'stack')

    return go.Figure(data=[in_stock, reorder_level], layout=layout)

if __name__ == '__main__':
    # print(get_completed_deliveries_dict())
    # print(get_late_deliveries_dict())
    # print(get_on_time_deliveries_dict())
    # print(get_undelivered_dict())
    # pyo.plot(get_amt_sold_over_time_bubble())
    # pyo.plot(get_total_sales_over_time_series())
    # pyo.plot(get_total_sales_per_country_heatmap())
    # pyo.plot(get_num_of_shipped_items_per_shipper_pie())
    # pyo.plot(get_backorder_bar())
    pyo.plot(get_reorder_bar())

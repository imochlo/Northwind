import numpy as np
import pandas as pd
import plotly.offline as pyo
import plotly.graph_objs as go
import pycountry

def amountsold_vs_country():
    df = pd.read_sql_table("D_OrderDetail", "sqlite:///../DW/DW.sqlite")
    df = df.groupby(["ShipCountry"], as_index=False).agg({"Quantity":"sum"})
    countries = {}
    def get_code(c):
        for country in pycountry.countries:
            if c in country.name or c == country.alpha_3:
                return country.alpha_3

    df.replace({"ShipCountry":"UK"}, {"ShipCountry":"United Kingdom"}, inplace=True)
    df["Code"] = df["ShipCountry"].astype(str).apply(lambda country: get_code(country))

    data = go.Choropleth(
        locations = df['Code'],
        z = df['Quantity'],
        text = df['ShipCountry'],
        colorscale = 'Blues',
        autocolorscale=False,
        reversescale=True,
        marker = dict(line=dict(width=0.5))
        # colorbar_tickprefix = '$',
        # colorbar_title = 'GDP<br>Billions US$',
    )

    layout = go.Layout(
        title='2014 Global GDP',
        geo=dict( showframe=False),
    )
    fig = go.Figure(data=[data], layout=layout)

    pyo.plot(fig, filename='numOfOrderedProducts_vs_Time')

if __name__ == "__main__":
    amountsold_vs_country()

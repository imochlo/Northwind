import dash_html_components as html
import dash_core_components as dcc
import dash_admin_components as dac

from Model import get_total_sales_per_country_heatmap, get_amt_sold_over_time_bubble, get_total_sales_over_time_series

sales_tab = dac.TabItem(id='content_sales', 
                              
    children=[
     
        html.Div(
            [
                    
                dac.SimpleBox(
                        # style = {'height': "600px"},
                    # title = "Total Sales per Country",
                    children=[
                        dcc.Graph(
                            figure=get_total_sales_per_country_heatmap(),
                            config=dict(displayModeBar=False),
                            style={'width': '38vw'}
                        )
                    ]
                ),
                        
                dac.SimpleBox(
                        # style = {'height': "600px"},
                    # title = "Total Sales per Country",
                    children=[
                        dcc.Graph(
                            figure=get_amt_sold_over_time_bubble(),
                            config=dict(displayModeBar=False),
                            style={'width': '38vw'}
                        )
                    ]
                ),
            ], 
            className='row'
        ),
                        
        html.Div(    
            dac.SimpleBox(
                    # style = {'height': "600px"},
                # title = "Total Sales per Country",
                children=[
                    dcc.Graph(
                        figure=get_total_sales_over_time_series(),
                        config=dict(displayModeBar=False),
                        style={'width': '38vw'}
                    )
                ]
            ),
            className='row'
        )
            
    ]
)

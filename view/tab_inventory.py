import dash_html_components as html
import dash_admin_components as dac

import dash_core_components as dcc
from Model import get_reorder_bar, get_backorder_bar

inventory_tab = dac.TabItem(id='content_inventory', 
                              
    children=[
        html.Div(
            [
                    
                dac.SimpleBox(
                        # style = {'height': "600px"},
                    # title = "Total Sales per Country",
                    children=[
                        dcc.Graph(
                            figure=get_reorder_bar(),
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
                            figure=get_backorder_bar(),
                            config=dict(displayModeBar=False),
                            style={'width': '38vw'}
                        )
                    ]
                ),
            ], 
            className='row'
        ),
    ]
)

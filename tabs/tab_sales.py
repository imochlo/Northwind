import dash_html_components as html
import dash_core_components as dcc
import dash_admin_components as dac

from plots.plots import plot_pie, plot_surface, plot_scatter
from Model import get_total_sales_per_country_heatmap, get_amt_sold_over_time_bubble

dropdown_items = [
	dac.BoxDropdownItem(url="https://www.google.com", children="Link to google"),
	dac.BoxDropdownItem(url="#", children="item 2"),
	dac.BoxDropdownDivider(),
	dac.BoxDropdownItem(url="#", children="item 3")
]

sales_tab = dac.TabItem(id='content_sales', 
                              
    children=[
     
        html.Div(
            [
                    
                dac.Box(
                    [
                        dac.BoxHeader(
                            dac.BoxDropdown(dropdown_items),
                            collapsible = True,
                            closable = True,
                            title = "Total Sales per Country",
                        ),
                    	dac.BoxBody(
                            dcc.Graph(
                                figure=get_total_sales_per_country_heatmap(),
                                config=dict(displayModeBar=False),
                                style={'width': '38vw'}
                            )		
                        )
                    ],
                    color='success',
                    width=6
                ),
                        
                dac.Box(
                    [
                        dac.BoxHeader(
                            collapsible = True,
                            closable = True,
                            title="Number of Items Sold per Region Over Time"
                        ),
                    	dac.BoxBody(
                            dcc.Graph(
                                figure=get_amt_sold_over_time_bubble(),
                                config=dict(displayModeBar=False),
                                style={'width': '38vw'}
                            )
                        )		
                    ],
                    color='warning',
                    width=6
                )
            ], 
            className='row'
        ),
                        
        html.Div(    
            dac.SimpleBox(
            	style = {'height': "600px"},
                title = "Total Sales per Country",
                children=[
                    dcc.Graph(
                        figure=get_total_sales_per_country_heatmap(),
                        config=dict(displayModeBar=False),
                        style={'width': '38vw'}
                    )
                ]
            ),
            className='row'
        )
            
    ]
)

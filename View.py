import dash
from dash.dependencies import Input, Output

import dash_html_components as html
import dash_core_components as dcc
import dash_admin_components as dac

from dash.exceptions import PreventUpdate

from tabs.tab_sales import sales_tab
from tabs.tab_shipment import shipment_tab
from tabs.tab_inventory import inventory_tab

from plots.plots import plot_scatter
from tabs.tab_inventory import text_1, text_2, text_3

# =============================================================================
# Dash App and Flask Server
# =============================================================================
app = dash.Dash(__name__)
server = app.server 

# =============================================================================
# Dash Admin Components
# =============================================================================
# Navbar
right_ui = dac.NavbarDropdown(
	badge_label = "!",
    badge_color= "danger",
    src = "https://quantee.ai",
	header_text="2 Items",
    children= [
		dac.NavbarDropdownItem(
			children = "message 1",
			date = "today"
		),
		dac.NavbarDropdownItem(
			children = "message 2",
			date = "yesterday"
		),
	]
)
                              
navbar = dac.Navbar(color = "white", 
                    children=right_ui)

# Sidebar
sidebar = dac.Sidebar(
	dac.SidebarMenu(
		[
                    dac.SidebarMenuItem(id='tab_sales', label='Sales', icon='dollar-sign'),
                    dac.SidebarMenuItem(id='tab_shipment', label='Shipment', icon='truck'),
                    dac.SidebarMenuItem(id='tab_inventory', label='Inventory', icon='boxes'),
		]
	),
    title='Nortwind Admin',
    skin="light",
    color="primary",
    brand_color="primary",
    url="https://quantee.ai",
    src="https://static.thenounproject.com/png/214846-200.png",
    elevation=3,
    opacity=0.8
)

# Body
body = dac.Body(
    dac.TabItems([
        sales_tab,
        shipment_tab,
        inventory_tab,
    ])
)

# Controlbar
# controlbar = dac.Controlbar(
    # [
        # html.Br(),
        # html.P("Slide to change graph in Basic Boxes"),
        # dcc.Slider(
            # id='controlbar-slider',
            # min=10,
            # max=50,
            # step=1,
            # value=20
        # )
    # ],
    # title = "My right sidebar",
    # skin = "light"
# )

# Footer
footer = dac.Footer(
	html.A("@DawidKopczyk, Quantee",
		href = "https://twitter.com/quanteeai", 
		target = "_blank", 
	),
	right_text = "2019"
)

# =============================================================================
# App Layout
# =============================================================================
app.layout = dac.Page([navbar, sidebar, body, footer])

# =============================================================================
# Callbacks
# =============================================================================
def activate(input_id, n_sales, n_shipment, n_inventory):
    
    # Depending on tab which triggered a callback, show/hide contents of app
    if input_id == 'tab_sales' and n_sales:
        return True, False, False
    elif input_id == 'tab_shipment' and n_shipment:
        return False, True, False 
    elif input_id == 'tab_inventory' and n_inventory:
        return False, False, True
    else:
        return True, False, False
    
@app.callback([Output('content_sales', 'active'),
               Output('content_shipment', 'active'),
               Output('content_inventory', 'active')],
               [Input('tab_sales', 'n_clicks'),
                Input('tab_shipment', 'n_clicks'),
                Input('tab_inventory', 'n_clicks')]
)
def display_tab(n_sales, n_shipment, n_inventory):
    
    ctx = dash.callback_context # Callback context to recognize which input has been triggered

    # Get id of input which triggered callback  
    if not ctx.triggered:
        raise PreventUpdate
    else:
        input_id = ctx.triggered[0]['prop_id'].split('.')[0]   

    return activate(input_id, n_sales, n_shipment, n_inventory)

@app.callback([Output('tab_sales', 'active'),
               Output('tab_shipment', 'active'),
               Output('tab_inventory', 'active')],
               [Input('tab_sales', 'n_clicks'),
                Input('tab_shipment', 'n_clicks'),
                Input('tab_inventory', 'n_clicks')]
)
def activate_tab(n_sales, n_shipment, n_inventory):
    
    ctx = dash.callback_context # Callback context to recognize which input has been triggered

    # Get id of input which triggered callback  
    if not ctx.triggered:
        raise PreventUpdate
    else:
        input_id = ctx.triggered[0]['prop_id'].split('.')[0]   

    return activate(input_id, n_sales, n_shipment, n_inventory)
    
@app.callback(Output('tab_box_1', 'children'),
              [Input('tab_box_1_menu', 'active_tab')]
)
def display_tabbox1(active_tab):

    # Depending on tab which triggered a callback, show/hide contents of app
    if active_tab == 'tab_box_1_tab1':
        return text_1
    elif active_tab == 'tab_box_1_tab2':
        return text_2
    elif active_tab == 'tab_box_1_tab3':
        return text_3

@app.callback(Output('tab_box_2', 'children'),
              [Input('tab_box_2_menu', 'active_tab')]
)
def display_tabbox2(active_tab):

    # Depending on tab which triggered a callback, show/hide contents of app
    if active_tab == 'tab_box_2_tab1':
        return text_1
    elif active_tab == 'tab_box_2_tab2':
        return text_2
    elif active_tab == 'tab_box_2_tab3':
        return text_3
    
# Update figure on slider change
# @app.callback(
    # Output('box-graph', 'figure'),
    # [Input('controlbar-slider', 'value')])
# def update_box_graph(value):
    # return plot_scatter(value)

# =============================================================================
# Run app    
# =============================================================================
if __name__ == '__main__':
    app.run_server(debug=True, port=8000)

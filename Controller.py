import dash
from dash.dependencies import Input, Output

import dash_html_components as html
import dash_core_components as dcc
import dash_admin_components as dac

from dash.exceptions import PreventUpdate

from view.tab_sales import sales_tab
from view.tab_shipment import shipment_tab
from view.tab_inventory import inventory_tab

# =============================================================================
# Dash App and Flask Server
# =============================================================================
app = dash.Dash(__name__)
server = app.server 

# =============================================================================
# Dash Admin Components
# =============================================================================
# Navbar
navbar = dac.Navbar(color = "white")

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

# Footer
footer = dac.Footer(
	html.A("Developed by Sam Solis",
		href = "https://twitter.com/ohsamsolis", 
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
    
# =============================================================================
# Run app    
# =============================================================================
if __name__ == '__main__':
    app.run_server(debug=True, port=8000)

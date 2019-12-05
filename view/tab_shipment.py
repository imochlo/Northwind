import dash_html_components as html
import dash_admin_components as dac

import dash_core_components as dcc
from Model import get_completed_deliveries_dict, get_late_deliveries_dict, get_on_time_deliveries_dict, get_num_of_shipped_items_per_shipper_pie

completed_value = get_completed_deliveries_dict()["value"]
completed_perc = get_completed_deliveries_dict()["percentage"]
late_value = get_late_deliveries_dict()["value"]
late_perc = get_late_deliveries_dict()["percentage"]
ontime_value = get_on_time_deliveries_dict()["value"]
ontime_perc = get_on_time_deliveries_dict()["percentage"]

shipment_tab = dac.TabItem(id='content_shipment', 
                              
    children=[
     
        html.Div(
            [
                dac.ValueBox(
                    value=str(completed_value) + " (" + str(completed_perc) + ")",
                    subtitle="Completed Deliveries",
                    color = "success",
                    icon = "check-double",
                    # href = "#"
                ),
                dac.ValueBox(
                    value=str(ontime_value) + " (" + str(ontime_perc) + ")",
                    subtitle="On Time Deliveries",
                    color = "primary",
                    icon = "calendar-check",
                    # href = "#"
                ),
                dac.ValueBox(
                    value=str(late_value) + " (" + str(late_perc) + ")",
                    subtitle="Late Deliveries",
                    color = "danger",
                    icon = "calendar-times",
                    # href = "#"
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
                        figure=get_num_of_shipped_items_per_shipper_pie(),
                        config=dict(displayModeBar=False),
                        style={'width': '38vw'}
                    )
                ]
            ),
            className='row'
        )
    ]
)

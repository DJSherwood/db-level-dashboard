import os
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go

# Data Load Section
########################################
# def load_data_s3():
#     S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
#     S3_REGION = os.environ.get('S3_REGION', 'us-east-1')
#     S3_PREFIX = os.environ.get('S3_PREFIX', 'merged_data/')
#
#     # load data frame
#     key = f"{S3_PREFIX}{'primary_data.csv'}"
#     s3_client = boto3.client('s3', region_name=S3_REGION)
#
#     try:
#         obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
#         df = pd.read_csv(obj['Body'])
#         # convert timestamp to time and date
#         df['datetime'] = pd.to_datetime(df['time'].astype('int'), unit='s') - pd.Timedelta(hours=5)
#
#     except Exception as e:
#         df = pd.DataFrame()
#
#     return df

# load data frame
def load_data():
    df = pd.read_csv('primary_data.csv')
    # convert timestamp to time and date
    df['datetime'] = pd.to_datetime(df['time'].astype('int'), unit='s') - pd.Timedelta(hours=5)
    return df

# Figure Definition Section
#########################################
# Create the gauge figure
gauge_figure = go.Figure(go.Indicator(
    domain={'x': [0, 1], 'y': [0, 1]},
    value=55,
    mode="gauge+number+delta",
    title={'text': 'Max Decibel Level'},
    delta={'reference': 55},
    gauge={'axis': {'range': [20, 120]},
           'steps': [
               {'range': [0, 65], 'color': "lightgreen"},
               {'range': [66, 85], 'color': "lightyellow"},
               {'range': [86, 120], 'color': "lightcoral"}
           ],
           'threshold': {'line': {'color': "black" , 'width': 4}, 'thickness': 0.75, 'value': 55}
           }
))
# Create the heatmap figure
heatmap_figure = go.Figure(go.Heatmap(
    z=[[ 0 for _ in range(31)] for _ in range(3)],
    x=[str(x) for x in range(1, 32)],
    y=['May', 'June', 'July'],
    colorscale='rdylbu',
    showscale=False
))

# App Definition and Layout
########################################
app = dash.Dash(__name__)
# for elastic beanstalk
# application = app.server
# Create layout without using dash-bootstrap-components
app.layout = html.Div([
    html.Div([
        html.Button('Refresh Data', id='refresh-button', className='btn btn-primary')
    ]),
    # Header
    html.H2("Search for Decibel Level by Time", style={'textAlign': 'center', 'margin': '20px'}),
    # top portion - Gauge
    html.Div([
        # Selector Container
        html.Div([
            html.P("Date Selectors"),
            # populating this dynamically
            dcc.Dropdown(id='crossfilter-time-aggregation', value=None, clearable=True,className="mr-3")
        ], style={'flex': 1}, className='shaded-div'),
        # Graph container
        html.Div([
            dcc.Graph(
                id='decibel-gauge',
                figure=gauge_figure
            ),
        ], style={'flex': 2}, className='shaded-div')
    ], style={'display': 'flex', 'flexDirection': 'row', 'justifyContent': 'space-between'}),
    # Header
    html.H2("Search for Time by Decibel Level", style={'textAlign': 'center', 'margin': '20px'}),
   # bottom portion - Heatmap
    html.Div([
        # Selector Container
        html.Div([
            html.P("Decibel Selector"),
            # can be static
            dcc.Slider(id='crossfilter-decibel-level', min=40, max=90, step=5, value=None, className='mr-3')
        ], style={'flex':1}, className='shaded-div'),
        # Graph Container
       html.Div([
            dcc.Graph(
                id='decibel-heatmap',
                figure=heatmap_figure
            ),
       ], style={'flex': 2}, className='shaded-div')
    ], style={'display': 'flex', 'flexDirection': 'row', 'justifyContent': 'space-between'}),
    # interesting use this to store data?
    dcc.Store(id='stored-data'),
    # auto refresh
    dcc.Interval(
        id='interval-component',
        interval=60_000,
        n_intervals=0
    )
], style={'fontFamily': 'Arial', 'maxWidth': '1200px', 'margin': '0 auto', 'padding': '0 15px'})

# Callbacks
########################################
# refresh the selector with data from the S3 bucket
@app.callback(
    [
        Output('stored-data', 'data'),
       # Output('last-update-time', 'children'),
        Output('crossfilter-time-aggregation', 'options'),
    ],
    [
        Input('refresh-button','n_clicks'),
        Input('interval-component','n_intervals')
    ])
def update_data_from_s3(n_clicks, n_intervals):
    df = load_data()
    df['day'] = pd.DatetimeIndex(df['datetime']).day
    df['month'] = pd.DatetimeIndex(df['datetime']).month
    day_list = df['day'].unique().tolist()
    options = [{'label': 'Day ' + str(i), 'value': i} for i in day_list]
    return df.to_dict('records'), options

@app.callback(
    Output('crossfilter-time-aggregation', 'value'),
    Input('crossfilter-time-aggregation', 'options'),
    State('crossfilter-time-aggregation', 'value')
)
def update_day_selector(available_options, current_value):
    if not available_options:
        return None
    # if current value is in options, keep it
    if current_value is not None:
        values = [option['value'] for option in available_options]
        if current_value in values:
            return current_value
    # otherwise return None (no selection)
    return None

# Make the gauge change with the selection of the dropdown
@app.callback(
    Output('decibel-gauge', 'figure'),
    [
        Input('stored-data', 'data'),
        Input('crossfilter-time-aggregation', 'value'),
    ])
def update_gauge_from_selector_and_s3(data, selected_day):
    if not data:
        raise PreventUpdate
    # yup
    df = pd.DataFrame(data)
    # use the value from the selector to aggregate
    filtered_df = df[ df.day == selected_day][['day','db_level']]
    # test
    if filtered_df.empty:
        m = 0
    else:
        m = filtered_df.groupby('day').agg({'db_level':'max'}).values[0][0]
    # create figure
    fig = go.Figure(go.Indicator(
    domain={'x': [0, 1], 'y': [0, 1]},
    value=m,
    mode="gauge+number+delta",
    title={'text': 'Maximum Decibel Level'},
    delta={'reference': 55},
    gauge={'axis': {'range': [20, 120]},
           'steps': [
               {'range': [0, 65], 'color': "lightgreen"},
               {'range': [66, 85], 'color': "lightyellow"},
               {'range': [86, 120], 'color': "lightcoral"}
           ],
           'threshold': {'line': {'color': "black" , 'width': 4}, 'thickness': 0.75, 'value': 55}
           }
    ))
    return fig

@app.callback(
    Output('decibel-heatmap', 'figure'),
    [
        Input('stored-data', 'data'),
        Input('crossfilter-decibel-level', 'value')
    ])
def update_heatmap_from_selector_and_s3(data, selected_decibel):
    if not data:
        raise PreventUpdate
    # convert dictionary to dataframe
    df = pd.DataFrame(data)
    # use the value from the selector to aggregate
    df2 = df[ df.db_level >= selected_decibel]
    unique_months = df2['month'].unique().tolist()
    unique_days = df2['day'].unique().tolist()
    df3 = df2.groupby(['month', 'day']).agg({'db_level':'max'}).reset_index()
    df4 = df3.pivot(index='month',columns='day',values='db_level').fillna(0)
    z = df4.values.tolist()
    # create figure
    fig = go.Figure(go.Heatmap(
        z=z,
        zmin=40,
        zmax=85,
        x=['Day ' + str(x) for x in unique_days],
        y=['Month ' + str(y) for y in unique_months],
        text=z,
        texttemplate="%{text}",
        colorscale='rdylbu',
        showscale=False
    ))
    return fig

if __name__ == '__main__':
    # port = int(os.environ.get("PORT", 8050))
    # local
    app.run(debug=True)
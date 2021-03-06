import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import base64
from datetime import datetime

# Importing process DB
df = pd.read_pickle('P:/8 Staff General/Ben Chapman/Opto/#113/2 DB/DB.pickle')

# Creating app
app = dash.Dash()

# P2i image

p2i_image = 'C:/Users/ben.chapman/Desktop/FTP/dashboard/P2i_strapline_RGB.png'
encoded_p2i_image = encoded_image = base64.b64encode(open(p2i_image, 'rb').read())

# Generating process options
Date_Options = []
for date in df['Date'].unique():
    Date_Options.append({'label' : str(date)[0:10], 'value' : str(date)[0:10]}) # requires slicing as it includes all of the 00:00:00 from the pd.Timestamp

Run_Options = []
for run in df['Run#'].unique():
    Run_Options.append({'label' : str(run), 'value' : run})

IO_Options = []
for io in df.columns[3:-2]:
    IO_Options.append({'label' : str(io), 'value' : io})

# App Layout

app.layout = html.Div([
                html.Div([
                        html.Div(html.Img(src='data:image/png;base64,{}'.format(encoded_p2i_image.decode()), height= 80,
                            style= {'border-color' : 'white', 'border-style' : 'solid', 'border-width' : 25, 'border-radius' : 15, 'position' : 'relative', 'bottom' : 15}),
                            style= {'display': 'inline-block', 'padding-left' : 25, 'padding-top' : 25}),
                        html.Div(html.H2('Analysis Dashboard',
                            style= {'color' : 'white', 'font-family': 'Verdana'}),
                            style= {'display': 'inline-block', 'position' : 'relative', 'bottom' : 65, 'left' : 20})],
                            style= {'background-color' : 'rgba(0, 94, 153 ,0.8)', 'display': 'block', 'height' : 100, 'border-radius' : 3}),
                        html.Div(dcc.Graph(id= 'graph')),
                        html.Div(dcc.Dropdown(id= 'Date Options',
                                                options= Date_Options,
                                                multi= True)),
                        html.Div(dcc.Checklist(id= 'Run Options',
                                                options= Run_Options,
                                                values= [1])),
                        html.Div(dcc.Dropdown(id= 'IO Options',
                                                options= IO_Options))

])

@app.callback(Output('graph', 'figure'),
                [Input('Date Options', 'value'),
                Input('Run Options', 'values'),
                Input('IO Options', 'value')])

def graph_updater(date_options, run_options, io_options):

    traces = []

    for day in date_options:
        for run in run_options:

            date = pd.Timestamp(day)
            dff = df[df['Date'] == date]
            dfff = dff[dff['Run#'] == run]

            traces.append(go.Scatter(x= dfff['Process Time'],
                                    y= dfff[str(io_options)],
                                    mode= 'lines'))

    return {'data' : traces,
            'layout': go.Layout(title= 'My Graph')}


    #date = pd.Timestamp(date_options) # The 'value' needs to be converted to a pd.Timestamp object, otherwise they aren't comparable.
    #dff = df[df['Date'] == date]
    #dfff = dff[dff['Run#'] == run_options]

    #return {
    #    'data': [go.Scatter(
    #    x= dfff['Process Time'],
    #    y= dfff[str(io_options)],
    #    mode= 'lines'
    #    )],

    #    'layout': go.Layout(title= 'My Graph')}


if __name__ == '__main__':
    app.run_server()

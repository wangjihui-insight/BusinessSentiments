import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import psycopg2
import pandas as pd
import plotly.graph_objs as go

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


def readfile(filepath):
    try:
        file = open(filepath)
    except IOError:
        print(IOError)
        return
    result = file.read().splitlines()
    file.close()
    return result


def get_data(target="nike"):

    config = readfile("config.txt")
    db = "dbname=" +  config[0] + " " \
         + "user=" + config[1] + " " \
         + "host=" + config[2] + " " \
         + "password=" + config[3]
    try:
        conn = psycopg2.connect(db)
    except:
        print("I am unable to connect to the database")
        return

    cur = conn.cursor()

    cur.execute("SELECT month, positive, negative FROM sentiments where word = '{}'".format(target))
    entry = cur.fetchall()

    month = []
    pos = []
    neg = []

    for row in entry:
        month.append(row[0])
        pos.append(row[1])
        neg.append(row[2])

    df = pd.DataFrame(list(zip(month, pos, neg)), columns =['month', 'positive', 'negative'])
    df = df.sort_values(by = ['month'])

    return df



fig = go.Figure()

fig.update_layout(
    autosize=False,
    width=1000,
    height=700,
    margin=go.layout.Margin(
        l=50,
        r=50,
        b=100,
        t=100,
        pad=4
    ),
    paper_bgcolor="LightSteelBlue",
)


app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H2('Welcome to Business Sentiments'),

    dcc.Input(id='brand', value='', type='text'),
    html.Button(id="submit-button", n_clicks=0, children="Submit"),
    html.Div(id='instruction', children=''),

    html.Div(
	dcc.Graph(id="sentiments",
                  figure = fig
                 )
    )
])



@app.callback(
    [Output(component_id='sentiments', component_property='figure'),
     Output('instruction', 'children')],
    [Input("submit-button", "n_clicks")],
    [State("brand", "value")]
)


def update_picture(self, input_value):

    df = get_data(input_value)

    
    inst = ""

    if not input_value:
        inst = ""
    elif df.empty:
        inst = "Result not found in database. We are processing your request. The result will be ready in one hour. Please come back later."

    fig = go.Figure()

    trace_positive = go.Scatter(x=list(df.month),
                    y=list(df.positive),
                    name="Postive")
    trace_negative = go.Scatter(x=list(df.month),
		    y=list(df.negative),
		    name="Negative")
    fig.add_trace(trace_positive)
    fig.add_trace(trace_negative)

    fig.update_layout(
    	autosize=False,
    	width=1000,
    	height=700,
    	margin=go.layout.Margin(
            l=50,
            r=50,
            b=100,
            t=100,
            pad=4
        ),
    paper_bgcolor="LightSteelBlue",
    )


    return fig, inst
	

if __name__ == '__main__':
    app.run_server(debug=True)

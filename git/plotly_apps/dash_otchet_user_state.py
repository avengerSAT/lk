import pandas as pd
from datetime import datetime



from django_plotly_dash import DjangoDash
from dash.dependencies import Input, Output
import dash
try:
    from dash import dcc
    from dash import html   
except:
    import dash_core_components as dcc
    import dash_html_components as html
import plotly.express as px
from dash import dash_table
from datetime import date, datetime

app = DjangoDash('dash_otchet_user_state')
df = pd.DataFrame({
    "Fruit": ["Apples", "Oranges", "Bananas", "Apples", "Oranges", "Bananas"],
    "Amount": [4, 1, 2, 2, 4, 5],
    "City": ["SF", "SF", "SF", "Montreal", "Montreal", "Montreal"]
})
fig = px.bar(df, x="Fruit", y="Amount", color="City", barmode="group")
app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for your data.
    '''),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )
])
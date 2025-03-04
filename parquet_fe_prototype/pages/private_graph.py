from dash import html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd


def make_layout():
    df = pd.read_csv(
        "https://raw.githubusercontent.com/plotly/datasets/master/gapminder_unfiltered.csv"
    )

    # Requires Dash 2.17.0 or later
    layout = [
        html.H1(children="Graph for weirdos", style={"textAlign": "center"}),
        dcc.Dropdown(df.country.unique(), "Canada", id="dropdown-selection"),
        dcc.Graph(id="graph-content-b"),
    ]

    @callback(Output("graph-content-b", "figure"), Input("dropdown-selection", "value"))
    def update_graph(value):
        dff = df[df.country == value]
        return px.line(dff, x="year", y="pop")

    return layout

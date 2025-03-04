from dash import html


def make_layout():
    return html.Div(
        [
            html.H1("This is our Home page"),
            html.Div("This is our Home page content."),
        ]
    )

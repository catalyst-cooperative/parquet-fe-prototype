from dash import (
    Dash,
    html,
    dcc,
    page_container,
    page_registry,
    register_page,
)

from parquet_fe_prototype.pages import dummy_graph, home, private_graph


def initialize_dash(app):
    dash = Dash(server=app, url_base_pathname="/dash/", use_pages=True)

    register_page("home", path="/", layout=home.make_layout())
    register_page("dummy_graph", layout=dummy_graph.make_layout())
    register_page("private_graph", layout=private_graph.make_layout())

    dash.layout = html.Div(
        [
            html.H1("Multi-page app with Dash Pages"),
            html.Div(
                [
                    html.Div(
                        dcc.Link(
                            f"{page['name']} - {page['path']}",
                            href=page["relative_path"],
                        )
                    )
                    for page in page_registry.values()
                ]
            ),
            page_container,
        ]
    )

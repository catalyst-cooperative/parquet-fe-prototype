import json
import yaml
from dataclasses import asdict
from pathlib import Path

from flask import Flask, request, render_template
from flask_htmx import HTMX

from parquet_fe_prototype import datapackage_shim
from parquet_fe_prototype.duckdb_query import perspective_to_duckdb, PerspectiveFilters
from parquet_fe_prototype.search import initialize_index, run_search


def create_app():
    app = Flask("parquet_fe_prototype", instance_relative_config=True)
    htmx = HTMX()
    htmx.init_app(app)

    app.config.from_mapping(SECRET_KEY="dev")
    # TODO: in the future, just generate this metadata from PUDL.
    metadata_path = Path(app.root_path) / "internal" / "metadata.yml"
    with open(metadata_path) as f:
        datapackage = datapackage_shim.metadata_to_datapackage(yaml.safe_load(f))

    index = initialize_index(datapackage)

    @app.get("/search")
    def search():
        template = "partials/search_results.html" if htmx else "search.html"
        query = request.args.get("q")
        if query:
            resources = run_search(ix=index, raw_query=query)
        else:
            resources = datapackage.resources

        return render_template(template, resources=resources, query=query)

    @app.get("/api/duckdb")
    def duckdb():
        filter_json = request.args.get("perspective_filters")
        perspective_filters = PerspectiveFilters.model_validate_json(filter_json)
        duckdb_query = perspective_to_duckdb(perspective_filters)
        for_download = json.loads(request.args.get("forDownload"))
        if not for_download:
            duckdb_query.statement += " LIMIT 10000"
        return asdict(duckdb_query)

    return app

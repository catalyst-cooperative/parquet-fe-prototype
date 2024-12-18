import json
import yaml
from pathlib import Path

from flask import Flask, request, render_template

from parquet_fe_prototype import datapackage_shim


def create_app():
    app = Flask("parquet_fe_prototype", instance_relative_config=True)
    app.config.from_mapping(SECRET_KEY="dev")
    metadata_path = Path(app.root_path) / "internal" / "metadata.yml"
    with open(metadata_path) as f:
        datapackage = datapackage_shim.metadata_to_datapackage(yaml.safe_load(f))

    @app.get("/search", defaults={"query": None})
    @app.get("/search/<query>")
    def search(query):
        if query:
            resources = [resource for resource in datapackage.resources if query in resource.name]
            return render_template("search.html", resources=resources)
        # TODO: think about how to htmxify
        # if htmx: only render the inside.
        # otherwise: render the outside too.
        return render_template("search.html", resources=datapackage.resources)

    @app.get("/preview/<table>")
    def preview(table: str):
        return render_template("preview.html", table=table)

    def build_query_with_filters(table_name, filter):
        query = f"SELECT * FROM {table_name} WHERE "

        where_clauses = ["true"]

        unary_ops = {"is null", "is not null"}

        op_conversion_templates = {
            "==": "{col} = {autoincrement_value}",
        }

        type_converters = {
            "date": "strptime(?,'%Y-%m-%d')",
            "number": "CAST(? AS DOUBLE)",
        }

        for filter_rule in filter:
            col, op, val = filter_rule["filter"]
            col_type = filter_rule["type"]
            if val is None:
                continue
            autoincrement_value = type_converters.get(col_type, "?")
            if op in op_conversion_templates:
                where_clauses.append(
                    op_conversion_templates[op].format(
                        col=col, op=op, autoincrement_value=autoincrement_value
                    )
                )
            elif op.lower() in unary_ops:
                where_clauses.append(f"{col} {op}")
            else:
                where_clauses.append(f"{col} {op} {autoincrement_value}")

        query += " AND ".join(where_clauses)
        return query

    @app.get("/api/duckdb")
    def duckdb_query():
        base_url = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/stable/"
        filename = f"{request.args.get("tableName")}.parquet"
        # actually return the filename and url also so that we can register that in duckdb
        filter_json = request.args.get("filter", "[]")
        # get the schema and use it to create filters
        filter = json.loads(filter_json)
        query = build_query_with_filters(
            table_name=filename,
            filter=filter,
        )
        for_download = json.loads(request.args.get("forDownload"))
        if not for_download:
            query += " LIMIT 1000"
        print(f"{query=}")
        return query

    return app

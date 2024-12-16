import json

from flask import Flask, request, render_template


def create_app():
    app = Flask("parquet-fe-prototype", instance_relative_config=True)
    app.config.from_mapping(SECRET_KEY="dev")

    @app.get("/")
    def root():
        return "hi"

    @app.get("/preview/<table>")
    def preview(table: str):
        return render_template("preview.html", table=table)

    def build_query_with_filters(table_name, filter):
        query = f"SELECT * FROM {table_name} WHERE "

        where_clauses = ["true"]

        for col, op, val in filter:
            if val is None:
                continue
            if op == "==":
                op = "="
            if col == "report_date":
                where_clauses.append(f"{col} {op} strptime(?, '%Y-%m-%d')")
            else:
                where_clauses.append(f"{col} {op} ?")

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
        query = build_query_with_filters(table_name=filename, filter=filter)
        for_download = json.loads(request.args.get("forDownload"))
        if not for_download:
            query += " LIMIT 1000"
        print(f"{query=}")
        return query

    return app
